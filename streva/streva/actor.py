from functools import wraps
import logging
import threading
import time
import traceback

from streva.reactor import NORMAL, URGENT
from streva.question import Questionnaire


class ErrorContext:

    def __init__(self, actor, event_name, message, error):
        self.actor = actor
        self.event_name = event_name
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
{}""".format(self.actor.name, self.event_name,
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

    def __init__(self, reactor):
        self._p_reactor = reactor
        self._p_planned = set()
        self._p_stopped = False

        # Set up reactor's lifecycle observation
        self._p_reactor.add_observer("start", self.init)
        self._p_reactor.add_observer("end", self.terminate)

    def get_reactor(self):
        return self._p_reactor

    def init(self):
        pass

    def terminate(self):
        pass

    def flush(self):
        for f in self._p_planned:
            f.cancel()

    def send_stop(self):
        self.call(self.stop, schedule=URGENT)

    def stop(self):
        self.flush()
        self._p_stopped = True

    def call(self, function, *args, schedule=NORMAL, **kwargs):
        if not self._p_stopped:
            @wraps(function)
            def baked():
                function(*args, **kwargs)

            func = Cancellable(baked)

            def cleanup():
                self._p_planned.remove(func)
            func.add_cleanup_f(cleanup)

            self._p_planned.add(func)
            self._p_reactor.schedule(func, schedule)


class Port:
    """ Port is a named set of actors to all of which an outbound message will
    be sent through this port.

    Port implements pubsub routing.
    """

    def __init__(self, name):
        self.name = name
        self._targets = []

    def send(self, message):
        for target_actor, event_name in self._targets:
            target_actor.send(event_name, message)


class Actor(Process):

    def __init__(self, reactor, name):
        super().__init__(reactor)
        self.name = name

        self._a_handlers = {}
        self._a_ports = {}

    def add_handler(self, operation, handler):
        self._a_handlers[operation] = handler

    def add_port(self, port_name, port):
        self._a_ports[port_name] = port

    def make_port(self, port_name):
        port = Port(port_name)
        self.add_port(port_name, port)
        return port

    def connect(self, port_name, to_actor, to_operation):
        port = self._a_ports[port_name]
        port._targets.append((to_actor, to_operation))

    def _add_callback(self, operation, function, message, schedule=NORMAL):
        self.call(function, message, schedule=schedule)

    def add_timeout(self, function, delay, message=None):
        self._add_callback("_timeout", function, message, schedule=delay)

    def send(self, operation, message, respond=None, urgent=False):
        handler = self._a_handlers[operation]
        schedule = URGENT if urgent else NORMAL

        f = handler
        if respond is not None:
            sender, callback = respond

            @wraps(handler)
            def resp_wrap(msg):
                handler(msg)
                sender._add_callback("_response", callback, self, schedule=schedule)
            f = resp_wrap

        self._add_callback(operation, f, message, schedule=schedule)


class HookedMixin(Actor):

    def __init__(self, reactor, name):
        super().__init__(reactor, name)
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

    def __init__(self, reactor, name):
        super().__init__(reactor, name)
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

        super()._add_callback(operation, try_function, message, schedule)



class SupervisorMixin(Actor):

    def __init__(self, reactor, name, probe_period=30, timeout_period=10):
        self._supervised_actors = set()

        self._ping_q = Questionnaire(self, ok=self.ping_ok, fail=self.ping_fail)
        self._stop_q = Questionnaire(self, ok=self.stop_ok, fail=self.stop_fail)

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


        super().__init__(reactor, name)

        self.get_reactor().add_observer("start", self.init_probe_cycle)
        self.add_handler("_error", self.error_received)

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

    # Stopping
    def stop_children(self):
        if not self.stop_sent:
            self.stop_sent = True
            for actor in self.get_supervised():
                self._stop_q.pose(actor, "_stop", None, urgent=True, timeout=self._failure_timeout_period)

    def stop_fail(self, actor):
        self.not_responding(actor)
        if self._stop_q.is_empty():
            self.all_stopped(None)

    def stop_ok(self, actor):
        if self._stop_q.is_empty():
            self.all_stopped(None)

    def all_stopped(self, msg):
        pass


    # Supervisor processes
    def init_probe_cycle(self):
        def probe(_):
            if not self.stop_sent:
                self._ping_q.clear()
                for actor in self._supervised_actors:
                    self._ping_q.pose(actor, "_ping", None, urgent=True, timeout=self._failure_timeout_period)

                # Loop pattern: repeat probing process
                self.add_timeout(probe, self._probe_period)
        self.add_timeout(probe, self._probe_period)

    def ping_ok(self, respondent):
        pass

    def ping_fail(self, respondent):
        self.not_responding(respondent)


    # Not responding and error handlers
    def not_responding(self, actor):
        name = actor.name
        logging.error("Actor '{}' hasn't responded in {} seconds!".format(name,
                                                                    self._failure_timeout_period))


class Stats:

    def __init__(self, event_name):
        self.event_name = event_name
        self.runs = 0
        self.processing_time = 0
        self.waiting_time = 0
        self.total_time = 0

    def __str__(self):
        avg_processing = self.processing_time / self.runs if self.runs else 0
        avg_waiting = self.waiting_time / self.runs if self.runs else 0
        avg_total = self.total_time / self.runs if self.runs else 0

        return \
"""+ {}
    Processing time:  {:.4f}[s] / {} = {:.4f}[s]
    Proc+Wait time:   {:.4f}[s] / {} = {:.4f}[s]""".format(self.event_name,
           self.processing_time, self.runs, avg_processing,
           self.total_time, self.runs, avg_total)

    def add(self, other):
        self.runs += other.runs
        self.processing_time += other.processing_time
        self.waiting_time += other.waiting_time
        self.total_time += other.total_time


class MeasuredMixin(HookedMixin, Actor):

    class Execution:

        def __init__(self):
            self.planned_at = None
            self.started_at = None

    def __init__(self, reactor, name):
        super().__init__(reactor, name)

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
        stats.processing_time += now - execution.started_at
        stats.total_time += now - execution.planned_at
        stats.waiting_time = stats.total_time - stats.processing_time

        self.last_updated = now

    def get_stats(self):
        return tuple(sorted(tuple(self._stats.items()), key=lambda t: t[1].runs))

    def get_total_stats(self):
        total = Stats("Total")
        for name, stats in self.get_stats():
            total.add(stats)
        return total

    def print_stats(self):
        print("\n# STATS for actor '{}':".format(self.name))
        print("sorted by number of runs. (total time[s]/runs = avg time[s])")
        for name, stats in self.get_stats():
            print(stats)
        print(self.get_total_stats())

