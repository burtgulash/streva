from functools import wraps
import logging
import time
import traceback

from streva.reactor import Event, NORMAL, URGENT
from streva.question import Questionnaire


class ErrorContext(Exception):

    def __init__(self, actor_name, event_name, message, err):
        super().__init__(err)

        self.actor_name = actor_name
        self.event_name = event_name
        self.message = message
        self.err = err

    def _exc_traceback(self, err):
        exc_info = type(err), err, err.__traceback__
        return "".join(traceback.format_exception(*exc_info))

    def __str__(self):
        return """

ERROR happened when actor  '{}.{}'  was sent message  '{}':
{}""".format(self.actor_name, self.event_name,
             str(self.message)[:30],
             self._exc_traceback(self.err))


class Process:

    def __init__(self, reactor):
        self._reactor = reactor
        self._planned = set()
        self.stopped = False

        # Set up reactor's lifecycle observation
        self._reactor.add_observer("start", self.init)
        self._reactor.add_observer("end", self.terminate)

    def init(self):
        pass

    def terminate(self):
        pass

    def after_cleanup(self):
        del self._planned[id_]

    def stop(self):
        self.call(flush, URGENT)
        self.stopped = True

    def call(self, function, schedule=NORMAL, *args, **kwargs):
        if not self.stopped:
            @wraps(function)
            def baked():
                function(*args, **kwargs)
            func = Cancellable(baked, self.after_cleanup)

            self._reactor.schedule(func, schedule)


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

        self._handlers = {}
        self._ports = {}

    def add_handler(self, operation, handler):
        self._handlers[operation] = handler

    def add_port(self, port_name, port):
        self._ports[port_name] = port

    def make_port(self, port_name):
        port = Port(port_name)
        self.add_port(port_name, port)
        return port

    def connect(self, port_name, to_actor, to_operation):
        port = self._ports[port_name]
        port._targets.append((to_actor, to_operation))

    def _add_callback(self, operation, function, message=None, schedule=NORMAL):
        self.call(function, message, schedule=schedule)

    def add_timeout(self, function, delay, message=None):
        self._add_callback("_timeout", function, message, schedule=delay)

    def send(self, operation, message, respond=None, urgent=False):
        handler = self._handlers[event_name]

        f = handler
        if respond is not None:
            sender, callback = respond

            @wraps(handler)
            def resp_wrap(msg):
                handler(msg)
                sender.add_callback("_response", callback, self, URGENT)
            f = resp_wrap

        self._add_callback(operation, f, message, URGENT if urgent else NORMAL)


class HookedMixin(Actor):

    class Id:
        pass

    def __init__(self, reactor, name):
        super().__init__(reactor, name)
        self._operations_map = {}

    def before_execute(self, event_id):
        pass

    def after_execute(self, event_id):
        pass

    def get_event_name(self, event_id):
        assert event_id in self._operations_map
        return self._operations_map[event_id]

    def _add_callback(self, operation, function, message, urgent=False):
        event_id = id(self.Id())

        @wraps(function)
        def hooked_function(message):
            self._operations_map[event_id] = operation

            self.before_execute(event_id)
            function(message)
            self.after_execute(event_id)

            del self._operations_map[event_id]

        super()._add_callback(operation, try_wrap, message, urgent)



class MonitoredMixin(HookedMixin, Actor):
    """ Allows the Actor object to be monitored by supervisors.
    """

    def __init__(self, reactor, name):
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
        self.stop()

    def on_error(self, err):
        # If there is no supervisor attached, then don't just pass the error
        # but raise it
        if not self.is_supervised():
            raise err

    def on_event_failed(self, event_id):
        event_name = self.get_event_name(event)

        error = ErrorContext(self.name, event_name, event.message, error)
        self.error_out.send(event)
        self.on_error(error)

    def _add_callback(self, operation, function, message, urgent=False):
        @wraps(function)
        def try_function(self, message):
            error = None
            try:
                function(message)
            except Exception as err:
                error = err

            if error is None:
                self.on_event_ok()
            else:
                self.on_event_failed()

        super()._add_callback(operation, try_function, message, urgent)



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

        self._reactor.add_observer("start", self.init_probe_cycle)
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

    def error_received(self, error_message):
        errored_event, error = error_message
        raise error

    # Stopping
    def stop_children(self):
        if not self.stop_sent:
            self.stop_sent = True
            for actor in self.get_supervised():
                self._stop_q.pose(actor, "_stop", None, urgent=True, timeout=self._failure_timeout_period)

    def stop_fail(self, actor):
        self.not_responding(actor)
        self.stop_ok(None)

    def stop_ok(self, actor):
        if self._stop_q.is_empty():
            self.all_stopped(None)

    def all_stopped(self, msg):
        pass


    # Supervisor processes
    def init_probe_cycle(self, msg):
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


class MeasuredMixin(HookedActor):

    def __init__(self, reactor, name):
        super().__init__(reactor, name)
        self._stats = {}
        self.last_updated = time.time()

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

    def register_event(self, event, event_name, schedule):
        if event_name not in self._stats:
            self._stats[event_name] = Stats(event_name)
        super().register_event(event, event_name, schedule)

    def _after_processed(self, event):
        event_name = self.get_event_name(event)
        super()._after_processed(event)
        self._collect_statistics(event_name, event)

    def make_event(self, function, message, delay=None):
        return self.MeasuredEvent(function, message, delay=delay)

    def _collect_statistics(self, event_name, event):
        stats = self._stats[event_name]
        now = time.time()

        stats.runs += 1
        stats.processing_time += now - event.processing_started_at
        stats.total_time += now - event.created_at
        stats.waiting_time = stats.total_time - stats.processing_time

        self.last_updated = now


    class MeasuredEvent(Event):

        def __init__(self, function, message, delay=None):
            super().__init__(function, message, delay=delay)

            self.created_at = time.time()
            self.processing_started_at = None

        def process(self):
            self.processing_started_at = time.time()
            Event.process(self)

