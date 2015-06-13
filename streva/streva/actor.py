from functools import wraps
import logging
import threading
import time
import traceback


from streva.reactor import TimedLoop, URGENT, NORMAL


class ErrorContext:

    def __init__(self, actor, operation, message, error):
        self.actor = actor
        self.operation = operation
        self.message = message
        self.error = error

    def get_exception(self):
        return self.error

    def _exc_traceback(self):
        error = self.error
        exc_info = type(error), error, error.__traceback__
        return "".join(traceback.format_exception(*exc_info))

    def __str__(self):
        return """

ERROR happened when actor  '{}.{}'  was sent message  '{}':
{}""".format(self.actor.name, self.operation,
             str(self.message)[:30],
             self._exc_traceback())


class Cancellable:

    __slots__ = "f", "cleanup", "cancelled", "finished"

    nop = lambda: None

    def __init__(self, f):
        self.f = f
        self.cleanup = self.nop
        self.cancelled = False
        self.finished = False

    def __call__(self):
        if not self.cancelled:
            self.f()
        self.cleanup()
        self.finished = True

    def __repr__(self):
        return "Cancellable({})".format(self.f)

    def add_cleanup_f(self, cleanup_f):
        self.cleanup = cleanup_f

    def cancel(self):
        self.finished = True

    def is_finished(self):
        return self.finished

    def is_canceled(self):
        return self.cancelled


class Process:

    def __init__(self, loop):
        self.__loop = loop
        self.__planned = set()
        self.__stopped = False

        # Set up loop's lifecycle observation
        self.__loop.add_observer("start", self.init)
        self.__loop.add_observer("end", self.terminate)

    def get_loop(self):
        return self.__loop

    def init(self):
        pass

    def terminate(self):
        pass

    def flush(self):
        for f in self.__planned:
            f.cancel()

    def send_stop(self):
        self.call(self.stop, schedule=URGENT)

    def stop(self):
        self.flush()
        self.__stopped = True

    def call(self, function, *args, schedule=NORMAL, **kwds):
        if not self.__stopped:
            @wraps(function)
            def baked():
                function(*args, **kwds)

            func = Cancellable(baked)

            def cleanup():
                self.__planned.remove(func)
            func.add_cleanup_f(cleanup)

            self.__planned.add(func)
            self.__loop.schedule(func, schedule)


class Port:
    """ Port is a named set of actors to all of which an outbound message will
    be sent through this port.

    Port implements pubsub routing.
    """

    def __init__(self, name):
        self.name = name
        self._targets = []

    def send(self, message):
        if not self._targets:
            raise Exception("Empty targets of port {}!".format(self.name))

        for target_actor, operation in self._targets:
            target_actor.send(operation, message)


class Actor(Process):

    def __init__(self, loop, name):
        super().__init__(loop)
        self.name = name

        self.__handlers = {}
        self.__ports = {}

    def add_handler(self, operation, handler):
        self.__handlers[operation] = handler

    def add_port(self, port_name, port):
        self.__ports[port_name] = port

    def make_port(self, port_name):
        port = Port(port_name)
        self.add_port(port_name, port)
        return port

    def connect(self, port_name, to_actor, to_operation):
        port = self.__ports[port_name]
        port._targets.append((to_actor, to_operation))

    def _add_callback(self, operation, function, message, schedule=NORMAL):
        self.call(function, message, schedule=schedule)

    def send(self, operation, message, respond=None, urgent=False):
        handler = self.__handlers[operation]
        schedule = URGENT if urgent else NORMAL

        f = handler
        if respond is not None:
            sender, respond_operation = respond

            @wraps(handler)
            def resp_wrap(msg):
                handler(msg)
                sender.send(respond_operation, self, urgent=urgent)
            f = resp_wrap

        self._add_callback(operation, f, message, schedule=schedule)


class HookedMixin(Actor):

    def __init__(self, loop, name):
        super().__init__(loop, name)
        self._h_lock = threading.Lock()

    class Id:
        pass

    def before_schedule(self, execution_id, operation, function, message):
        pass

    def before_execute(self, execution_id, operation, function, message):
        pass

    def after_execute(self, execution_id, operation, function, message):
        pass

    def _add_callback(self, operation, function, message, schedule=NORMAL):
        execution_id = id(self.Id())

        # Because _add_callback is the only function, which is called from
        # another thread and method self.before_schedule can modify this
        # actor's state, we need to put it into critical section
        with self._h_lock:
            self.before_schedule(execution_id, operation, function, message)

        @wraps(function)
        def hooked_function(message):
            self.before_execute(execution_id, operation, function, message)
            function(message)
            self.after_execute(execution_id, operation, function, message)

        super()._add_callback(operation, hooked_function, message, schedule=schedule)


class MonitoredMixin(Actor):
    """ Allows the Actor object to be monitored by supervisors.
    """

    def __init__(self, loop, name):
        super().__init__(loop, name)
        self.supervisor = None

        self.add_handler("_ping", self._on_ping)
        self.add_handler("_stop", self._on_stop)
        self.error_out = self.make_port("_error")

    def set_supervisor(self, supervisor):
        self.supervisor = supervisor

    def is_supervised(self):
        return self.supervisor is not None

    def _on_ping(self, msg):
        pass

    def _on_stop(self, msg):
        self.send_stop()

    def on_error(self, err):
        # If there is no supervisor attached, then don't just pass the error
        # but raise it
        if not self.is_supervised():
            raise err

    def _add_callback(self, operation, function, message, schedule=NORMAL):
        @wraps(function)
        def try_function(message):
            error = None
            try:
                function(message)
            except Exception as err:
                error = err

            if error is not None:
                error_context = ErrorContext(self, operation, message, error)
                self.error_out.send(error_context)
                self.on_error(error)

        super()._add_callback(operation, try_function, message, schedule=schedule)


class DelayableMixin(Actor):

    def __init__(self, loop, name):
        super().__init__(loop, name)
        self.__after_out = self.make_port("_after")

    def connect_timer(self, timer_actor):
        self.connect("_after", timer_actor, "_after")

    def delay(self, operation, after):
        self.__after_out.send((self, operation, after))


class TimerMixin(Actor):

    def __init__(self, loop, name):
        if not isinstance(loop, TimedLoop):
            raise TypeError("Loop for TimerMixin must be TimedLoop instance!")
        super().__init__(loop, name)
        self.add_handler("_after", self.__after)

    def add_timeout(self, callback, after, message=None):
        self._add_callback("_timeout", callback, message, schedule=after)

    def __after(self, msg):
        sender, operation, after = msg
        self.add_timeout(self.__delayed_send, after, message=(sender, operation))

    def __delayed_send(self, msg):
        sender, operation = msg
        sender.send(operation, None)


class Stats:

    class StatType:

        def __init__(self, type_name):
            self.type_name = type_name
            self.value = 0

        def avg(self, runs):
            return self.value / runs if runs else 0

        def add(self, value):
            self.value += value



    def __init__(self, operation):
        self.operation = operation
        self.runs = 0
        self.processing_time = self.StatType("processing time")
        self.waiting_time = self.StatType("waiting time")
        self.total_time = self.StatType("total time")

        self.time_types = self.processing_time, self.waiting_time, self.total_time

    def __str__(self):
        s = ["    {:22} {:.4f}[s] / {} = {:.4f}[s]".format(x.type_name.capitalize(),
                                                           x.value,
                                                           self.runs,
                                                           x.avg(self.runs))
             for x
             in self.time_types]

        return "+ {}\n{}".format(self.operation, "\n".join(s))

    def add(self, other):
        self.runs += other.runs
        for a, b in zip(self.time_types, other.time_types):
            a.add(b.value)


class MeasuredMixin(HookedMixin, Actor):

    class Execution:

        def __init__(self):
            self.planned_at = None
            self.started_at = None

    def __init__(self, loop, name):
        super().__init__(loop, name)

        self.last_updated = time.time()
        self._stats = {}
        self._hooked_events = {}

    def before_schedule(self, execution_id, operation, function, message):
        if operation not in self._stats:
            self._stats[operation] = Stats(operation)

        execution = self.Execution()
        execution.planned_at = time.time()

        self._hooked_events[execution_id] = execution

    def before_execute(self, execution_id, operation, function, message):
        self._hooked_events[execution_id].started_at = time.time()

    def after_execute(self, execution_id, operation, function, message):
        assert operation in self._stats
        assert execution_id in self._hooked_events

        now = time.time()

        stats = self._stats[operation]
        execution = self._hooked_events[execution_id]

        stats.runs += 1
        stats.processing_time.add(now - execution.started_at)
        stats.total_time.add(now - execution.planned_at)
        stats.waiting_time.value = stats.total_time.value - stats.processing_time.value

        self.last_updated = now

    def get_stats(self):
        return [s for s in sorted(self._stats.values(), key=lambda x: x.runs)]

    def get_total_stats(self):
        total = Stats("Total")
        for stats in self.get_stats():
            total.add(stats)
        return total

    def print_stats(self):
        print("\n# STATS for actor '{}':".format(self.name))
        print("sorted by number of runs. (total time[s]/runs = avg time[s])")
        for stats in self.get_stats():
            print(stats)
        print(self.get_total_stats())


class SupervisorMixin(DelayableMixin, Actor):

    def __init__(self, loop, name, probe_period=30, timeout_period=10):
        self._supervised_actors = set()

        self._ping_q = set()
        self._stop_q = set()

        # Probe all supervised actors regularly with this time period in
        # seconds
        self._probe_period = probe_period

        # Echo timeouts after this many seconds. Ie. this means the time period
        # after which a supervised actor should be recognized as dead
        self._failure_timeout_period = timeout_period

        # Make sure that all actors are evaluated of one round of probing
        # before next round of probing
        if not self._failure_timeout_period * 2 < self._probe_period:
            raise Exception("Timeout_period should be at most half the period of probe_period.")

        self.stop_sent = False


        super().__init__(loop, name)

        self.get_loop().add_observer("start", self.init_probe_cycle)

        self.add_handler("_error", self.error_received)
        self.add_handler("_stop_received", self._stop_received)
        self.add_handler("_pong", self._pong)

        self.add_handler("_stop_check_failures", self.stop_check_failures)
        self.add_handler("_ping_check_failures", self.ping_check_failures)
        self.add_handler("_probe", self._probe)

    def supervise(self, actor):
        if not isinstance(actor, MonitoredMixin):
            raise Exception("For the actor '{}' to be supervised, add MonitoredMixin to its base classes".
                    format(actor.name))

        self._supervised_actors.add(actor)
        actor.set_supervisor(self)
        actor.connect("_error", self, "_error")

    def get_supervised(self):
        return list(self._supervised_actors)

    def broadcast(self, operation, msg):
        for actor in self.get_supervised():
            actor.send(operation, msg)

    def error_received(self, error_context):
        actor, error = error_context.actor, error_context.err
        raise error

    def all_stopped(self, msg):
        pass

    # Stopping
    def stop_children(self):
        if not self.stop_sent:
            self.stop_sent = True

            self.delay("_stop_check_failures", self._failure_timeout_period)
            for actor in self.get_supervised():
                self._stop_q.add(actor)
                actor.send("_stop", None, respond=(self, "_stop_received"), urgent=True)

    def _stop_received(self, actor):
        if actor in self._stop_q:
            self._stop_q.remove(actor)
            if len(self._stop_q) == 0:
                self.all_stopped(None)

    def stop_check_failures(self, _):
        if len(self._stop_q) > 0:
            self._stop_q = set()
            logging.warning("""Killing everything ungracefully!
{} actors haven't responded to stop request in {} s.""".format(len(self._stop_q),
                                                               self._failure_timeout_period))
        self.all_stopped(None)

    # Supervisor ping process
    def init_probe_cycle(self):
        self.delay("_probe", self._probe_period)

    def _probe(self, msg):
        if not self.stop_sent:
            self._ping_q = set()

            for actor in self.get_supervised():
                self._ping_q.add(actor)
                actor.send("_ping", None, respond=(self, "_pong"), urgent=True)

            self.delay("_probe", self._probe_period)

    def _pong(self, actor):
        if actor in self._ping_q:
            self._ping_q.remove(actor)

    def ping_check_failures(self, _):
        for actor in self._ping_q:
            self._ping_q.remove(actor)
            logging.error("Actor '{}' hasn't responded in {} seconds!".format(actor.name,
                                                                    self._failure_timeout_period))

