from functools import wraps
import logging
import threading
import time
import traceback


from streva.reactor import Reactor, TimedReactor


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


class Enablable:

    def __init__(self):
        self.__active = False
        self.__waiting = []

    def __process_waiting(self):
        for message in self.__waiting:
            self._process(message)
        self.__waiting = []

    def _process(self, message):
        pass

    def _enqueue(self, message):
        self.__waiting.append(message)

    def active(self):
        return self.__active

    def activate(self):
        self.__active = True
        self.__process_waiting()

    def flush(self):
        self.__waiting = []

    def deactivate(self):
        self.__active = False


class Process(Enablable):

    def __init__(self):
        super().__init__()
        self.__reactor = None
        self.__planned = set()
        self.__stopped = True

    def get_reactor(self):
        return self.__reactor

    def set_reactor(self, reactor):
        self.__reactor = reactor
        self.activate()

    def unset_reactor(self):
        self.deactivate()
        self.__reactor = None

    def _process(self, message):
        if not self.__reactor:
            raise ValueError("Loop must be set before starting the process.!")

        function, when = message
        self.__planned.add(function)
        self.__reactor.receive(function, when)

    def terminate(self):
        pass

    def flush(self):
        super().flush()
        for f in self.__planned:
            f.cancel()

    def stop(self):
        self.call(self._stop)

    def start(self):
        self.__stopped = False

    def _stop(self):
        self.flush()
        self.terminate()
        self.__stopped = True

    def call(self, function, *args, when=Reactor.NOW, **kwds):
        @wraps(function)
        def baked():
            function(*args, **kwds)

        func = Cancellable(baked)

        def cleanup():
            self.__planned.remove(func)
        func.add_cleanup_f(cleanup)

        if not self.__stopped:
            if self.active():
                self._process((func, when))
            else:
                self._enqueue((func, when))


class Port(Enablable):
    """ Port is a named set of actors to all of which an outbound message will
    be sent through this port.

    Port implements pubsub routing.
    """

    def __init__(self, name):
        super().__init__()
        self.name = name
        self.__targets = []

    def add_target(self, actor, operation):
        self.__targets.append((actor, operation))

    def _process(self, message):
        for target_actor, operation in self.__targets:
            target_actor.send(operation, message)

    def send(self, message):
        if self.active():
            self._process(message)
        else:
            self._enqueue(message)


class Actor(Process):

    def __init__(self, name):
        super().__init__()
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

    def start(self):
        for port in self.__ports.values():
            port.activate()
        super().start()

    def connect(self, port_name, to_actor, to_operation):
        port = self.__ports[port_name]
        port.add_target(to_actor, to_operation)

    def _add_callback(self, operation, function, message, when=Reactor.NOW):
        self.call(function, message, when=when)

    def send(self, operation, message, respond=None):
        handler = self.__handlers[operation]

        f = handler
        if respond is not None:
            sender, respond_operation = respond

            @wraps(handler)
            def resp_wrap(msg):
                handler(msg)
                sender.send(respond_operation, self)
            f = resp_wrap

        self._add_callback(operation, f, message)


class InterceptedMixin(Actor):

    def __init__(self, name):
        super().__init__(name)
        self.__intercept_lock = threading.Lock()

    class Id:
        pass

    def before_schedule(self, execution_id, operation, function, message):
        pass

    def before_execute(self, execution_id, operation, function, message):
        pass

    def after_execute(self, execution_id, operation, function, message):
        pass

    def _add_callback(self, operation, function, message, when=Reactor.NOW):
        execution_id = id(self.Id())

        # Because _add_callback is the only function, which is called from
        # another thread and method self.before_schedule can modify this
        # actor's state, we need to put it into critical section
        with self.__intercept_lock:
            self.before_schedule(execution_id, operation, function, message)

        @wraps(function)
        def intercepted_function(message):
            self.before_execute(execution_id, operation, function, message)
            function(message)
            self.after_execute(execution_id, operation, function, message)

        super()._add_callback(operation, intercepted_function, message, when=when)


class MonitoredMixin(Actor):
    """ Allows the Actor object to be monitored by supervisors.
    """

    def __init__(self, name):
        super().__init__(name)
        self.supervisor = None

        self.add_handler("_ping", self.__on_ping)
        self.add_handler("_stop", self.__on_stop)
        self.error_out = self.make_port("_error")

    def set_supervisor(self, supervisor):
        self.supervisor = supervisor

    def is_supervised(self):
        return self.supervisor is not None

    def __on_ping(self, msg):
        pass

    def __on_stop(self, msg):
        self.stop()

    def on_error(self, err):
        # If there is no supervisor attached, then don't just pass the error
        # but raise it
        if not self.is_supervised():
            raise err

    def _add_callback(self, operation, function, message, when=Reactor.NOW):
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

        super()._add_callback(operation, try_function, message, when=when)


class DelayableMixin(Actor):

    def __init__(self, name):
        super().__init__(name)
        self.__after_out = self.make_port("_after")

    def connect_timer(self, timer_actor):
        self.connect("_after", timer_actor, "_after")

    def delay(self, operation, after):
        self.__after_out.send((self, operation, after))


class TimerMixin(Actor):

    def __init__(self, name):
        super().__init__(name)
        self.add_handler("_after", self.__after)

    def set_reactor(self, reactor):
        if not isinstance(reactor, TimedReactor):
            raise TypeError("Loop for TimerMixin must be TimedLoop instance!")
        super().set_reactor(reactor)

    def add_timeout(self, callback, after, message=None):
        self._add_callback("_timeout", callback, message, when=after)

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


class MeasuredMixin(InterceptedMixin, Actor):

    class Execution:

        def __init__(self):
            self.planned_at = None
            self.started_at = None

    def __init__(self, name):
        super().__init__(name)

        self.last_updated = time.time()
        self.__stats = {}
        self.__intercepted_events = {}

    def before_schedule(self, execution_id, operation, function, message):
        if operation not in self.__stats:
            self.__stats[operation] = Stats(operation)

        execution = self.Execution()
        execution.planned_at = time.time()

        self.__intercepted_events[execution_id] = execution

    def before_execute(self, execution_id, operation, function, message):
        self.__intercepted_events[execution_id].started_at = time.time()

    def after_execute(self, execution_id, operation, function, message):
        assert operation in self.__stats
        assert execution_id in self.__intercepted_events

        now = time.time()

        stats = self.__stats[operation]
        execution = self.__intercepted_events[execution_id]

        stats.runs += 1
        stats.processing_time.add(now - execution.started_at)
        stats.total_time.add(now - execution.planned_at)
        stats.waiting_time.value = stats.total_time.value - stats.processing_time.value

        self.last_updated = now

    def get_stats(self):
        return [s for s in sorted(self.__stats.values(), key=lambda x: x.runs)]

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

    def __init__(self, name, children=[], probe_period=30, timeout_period=10):
        self.__supervised_actors = set()

        self.__ping_q = set()
        self.__stop_q = set()

        # Probe all supervised actors regularly with this time period in
        # seconds
        self.__probe_period = probe_period

        # Echo timeouts after this many seconds. Ie. this means the time period
        # after which a supervised actor should be recognized as dead
        self.__failure_timeout_period = timeout_period

        # Make sure that all actors are evaluated of one round of probing
        # before next round of probing
        if not self.__failure_timeout_period * 2 < self.__probe_period:
            raise Exception("Timeout_period should be at most half the period of probe_period.")

        super().__init__(name)

        self.add_handler("_error", self.error_received)
        self.add_handler("_stop_received", self._stop_received)
        self.add_handler("_pong", self._pong)

        self.add_handler("_stop_check_failures", self.stop_check_failures)
        self.add_handler("_ping_check_failures", self.ping_check_failures)
        self.add_handler("_probe", self._probe)

        self.stop_sent = False
        for actor in children:
            self.supervise(actor)

        self.delay("_probe", self.__probe_period)

    def supervise(self, actor):
        if not isinstance(actor, MonitoredMixin):
            raise Exception("For the actor '{}' to be supervised, add MonitoredMixin to its base classes".
                    format(actor.name))

        self.__supervised_actors.add(actor)
        actor.set_supervisor(self)
        actor.connect("_error", self, "_error")

    def get_supervised(self):
        return list(self.__supervised_actors)

    def broadcast(self, operation, msg):
        for actor in self.get_supervised():
            actor.send(operation, msg)

    def error_received(self, error_context):
        actor, error = error_context.actor, error_context.err
        raise error

    def all_stopped(self, msg):
        self.stop()

    def start(self):
        super().start()
        for actor in self.get_supervised():
            actor.start()

    # Stopping
    def stop_children(self):
        if not self.stop_sent:
            self.stop_sent = True

            self.delay("_stop_check_failures", self.__failure_timeout_period)
            for actor in self.get_supervised():
                self.__stop_q.add(actor)
                actor.send("_stop", None, respond=(self, "_stop_received"))

    def _stop_received(self, actor):
        if actor in self.__stop_q:
            self.__stop_q.remove(actor)
            if len(self.__stop_q) == 0:
                self.all_stopped(None)

    def stop_check_failures(self, _):
        if len(self.__stop_q) > 0:
            self.__stop_q = set()
            logging.warning("""Killing everything ungracefully!
{} actors haven't responded to stop request in {} s.""".format(len(self.__stop_q),
                                                               self.__failure_timeout_period))
        self.all_stopped(None)

    def _probe(self, _):
        if not self.stop_sent:
            self.__ping_q = set()

            for actor in self.get_supervised():
                self.__ping_q.add(actor)
                actor.send("_ping", None, respond=(self, "_pong"))

            self.delay("_probe", self.__probe_period)

    def _pong(self, actor):
        if actor in self.__ping_q:
            self.__ping_q.remove(actor)

    def ping_check_failures(self, _):
        for actor in self.__ping_q:
            self.__ping_q.remove(actor)
            logging.error("Actor '{}' hasn't responded in {} seconds!".format(actor.name,
                                                                    self.__failure_timeout_period))

