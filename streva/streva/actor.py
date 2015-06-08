import logging
import time
import traceback

from streva.reactor import Event


class ErrorContext(Exception):

    def __init__(self, actor_name, event_name, message, err):
        super().__init__(err)

        self.actor_name = actor_name or "[actor]"
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


class Port:
    """ Port is a named set of actors to all of which an outbound message
    will be sent through this port. Port implements pubsub routing.
    """

    def __init__(self, name):
        self.name = name
        self._targets = []

    def send(self, message):
        """ Send message to all connected actors through this pubsub port.
        """
        for target_actor, event_name in self._targets:
            target_actor.send(event_name, message)


class Actor:
    """ Actor is a logical construct sitting upon Reactor, which it uses
    as its backend.

    Actors can route outgoing messages through Ports. Port is a publisher
    mechanism, which sends messages to its subscribers.
    """

    def __init__(self, reactor, name):
        self.name = name

        self._reactor = reactor

        self._events_planned = set()
        self._handlers = {}
        self._ports = {}

        # Set up reactor's lifecycle observation
        self._reactor.add_observer("start", self.init)
        self._reactor.add_observer("end", self.terminate)

        # Listen on lifecycle events
        self.add_handler("start", self.init)
        self.add_handler("end", self.terminate)
        self.add_handler("_question", self._question)

    # Actor lifecycle methods
    def init(self, message):
        pass

    def terminate(self, message):
        pass


    # Actor construction and setup methods
    def add_handler(self, event_name, handler):
        self._handlers[event_name] = handler

    def add_port(self, port_name, port):
        self._ports[port_name] = port

    def make_port(self, port_name):
        port = Port(port_name)
        self.add_port(port_name, port)
        return port

    def connect(self, port_name, to_actor, to_event_name):
        port = self._ports[port_name]
        port._targets.append((to_actor, to_event_name))


    # Actor diagnostic and control methods
    def flush(self):
        flushed_messages = []

        for event in self._events_planned:
            event.deactivate()
            flushed_messages.append(event.message)

        return flushed_messages

    def stop(self):
        # Stop processing new events by clearing all handlers
        self._handlers = {}

        # Clear all planned events
        return self.flush()

    def _question(self, msg):
        sender, function, message = msg
        response = function(message)
        sender.send("_response", (self, response))

    # Scheduling and sending methods
    def ask(self, sender, event_name, message, callback, urgent=False):
        handler = self._handlers[event_name]
        def qwrap(msg):
            handler(msg)
            sender.add_callback("_response", callback, self, urgent)
        self.add_callback(event_name, qwrap, message, -1 if urgent else 0)

    def send(self, event_name, message, urgent=False):
        handler = self._handlers[event_name]
        self.add_callback(event_name, handler, message, -1 if urgent else 0)

    def add_callback(self, event_name, function, message=None, schedule=0):
        event = self.make_event(function, message)
        self.register_event(event, event_name, schedule)

    def add_timeout(self, function, delay, message=None):
        self.add_callback("_timeout", function, message, schedule=delay)

    def make_event(self, function, message, delay=None):
        return Event(function, message, delay)

    def register_event(self, event, event_name, schedule):
        self._events_planned.add(event)

        # Extract the function from object first, otherwise it would be
        # evaluated when the function is likely changed
        f = event._function
        def wrap(message):
            self._callback(f, event.message, event)
            self._after_processed(event)

        event._function = wrap
        self._reactor.schedule(event, schedule)

    def _callback(self, function, message, event):
        function(message)

    def _after_processed(self, event):
        self._events_planned.remove(event)



class MonitoredMixin(Actor):
    """ Allows the Actor object to be monitored by supervisors.
    """

    def __init__(self, reactor, name, **kwargs):
        super().__init__(reactor=reactor, name=name, **kwargs)
        self._event_map = {}

        self.add_handler("_ping", self._ping)
        self.add_handler("_stop", self._on_stop)
        self.error_out = self.make_port("_error")
        self.is_supervised = False

    def _ping(self, msg):
        sender = msg
        sender.send("_pong", self)

    def _on_stop(self, msg):
        sender = msg
        self.stop()
        sender.send("_stop", self)

    def _err(self, error_message):
        errored_event, error = error_message
        event_name = self.get_event_name(errored_event)

        error = ErrorContext(self.name, event_name, errored_event.message, error)
        self.error_out.send(error_message)
        self.on_error(error)

    def _ok(self, event):
        assert event in self._event_map
        pass

    def register_event(self, event, event_name, schedule):
        self._event_map[event] = event_name
        super().register_event(event, event_name, schedule)

    def _callback(self, function, message, event):
        e = None
        try:
            function(message)
        except Exception as err:
            e = err

        if e is None:
            self._ok(event)
        else:
            self._err((event, e))

    def _after_processed(self, event):
        super()._after_processed(event)
        del self._event_map[event]

    def get_event_name(self, event):
        assert event in self._event_map
        return self._event_map[event]

    def on_error(self, err):
        # If there is no supervisor attached, then don't just pass the error
        # but raise it
        if not self.is_supervised:
            raise err


class SupervisorMixin(Actor):

    def __init__(self, reactor, name, probe_period=30, timeout_period=10, **kwargs):
        self._supervised_actors = set()
        self._ping_questions = set()
        self._stop_questions = set()

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


        super().__init__(reactor=reactor, name=name, **kwargs)
        self._reactor.add_observer("start", self.init_probe_cycle)
        self.add_handler("_error", self.error_received)
        self.add_handler("_pong", self._receive_pong)
        self.add_handler("_stop", self.receive_stop_confirmation)

    def supervise(self, actor):
        if not isinstance(actor, MonitoredMixin):
            raise Exception("For the actor '{}' to be supervised, add MonitoredMixin to its base classes".
                    format(actor.name))

        self._supervised_actors.add(actor)
        actor.connect("_error", self, "_error")
        actor.is_supervised = True

    def get_supervised(self):
        return list(self._supervised_actors)

    def broadcast(self, event_name, msg):
        for actor in self.get_supervised():
            actor.send(event_name, msg)

    def stop_children(self):
        def stop_timeout(_):
            if actor in self._stop_questions:
                self.not_responding(actor)
                self.stop_confirmation_processed(actor)

        if not self.stop_sent:
            self.stop_sent = True
            self.add_timeout(stop_timeout, self._failure_timeout_period)

            for actor in self.get_supervised():
                self._stop_questions.add(actor)
                actor.send("_stop", self, urgent=True)

    def stop_confirmation_processed(self, actor):
        self._stop_questions.remove(actor)
        if len(self._stop_questions) == 0:
            self.all_stopped(None)

    def receive_stop_confirmation(self, msg):
        sender = msg
        if sender in self._stop_questions:
            self.stop_confirmation_processed(sender)

    def all_stopped(self, msg):
        pass


    # Not responding and error handlers
    def not_responding(self, actor):
        name = actor.name or str(id(actor))
        logging.error("Actor '{}' hasn't responded in {} seconds!".format(name,
                                                                    self._failure_timeout_period))

    def error_received(self, error_message):
        errored_event, error = error_message
        raise error

    # Supervisor processes
    def init_probe_cycle(self, msg):
        def probe(_):
            # Reset ping questions
            self._ping_questions = set()

            if self.stop_sent:
                return

            for actor in self._supervised_actors:
                # Register question for probe of this actor
                self._ping_questions.add(actor)

                # Notice the message 'self'. Echoed actor uses that as a
                # recipient for this ping response
                actor.send("_ping", self)

                # Register timeout for this ping question
                self._register_failure_timeout(actor)

            # Loop pattern: repeat probing process
            self.add_timeout(probe, self._probe_period)
        self.add_timeout(probe, self._probe_period)


    def _register_failure_timeout(self, actor):
        def failure_timeout(_):
            # Failure timeout received for an actor, because it didn't respond
            # in time.
            if actor in self._ping_questions:
                self._ping_questions.remove(actor)

                self.not_responding(actor)

        self.add_timeout(failure_timeout, self._failure_timeout_period)

    def _receive_pong(self, msg):
        sender = msg

        # Failure timeout not yet received. Simply remove the question to
        # denote a success
        if sender in self._ping_questions:
            self._ping_questions.remove(sender)


class Stats:

    def __init__(self, event_name):
        self.event_name = event_name
        self.runs = 0
        self.processing_time = 0
        self.total_time = 0

    def __str__(self):
        avg_processing = self.processing_time / self.runs if self.runs else 0
        avg_total = self.total_time / self.runs if self.runs else 0

        return \
"""+ {}
    + Processing time:  {:.4f}[s] / {} = {:.4f}[s]
    + Total time:       {:.4f}[s] / {} = {:.4f}[s]""".format(self.event_name,
           self.processing_time, self.runs, avg_processing,
           self.total_time, self.runs, avg_total)


class MeasuredMixin(Actor):

    def __init__(self, reactor, name, **kwargs):
        self._stats = {}
        self.last_updated = time.time()
        super().__init__(reactor=reactor, name=name, **kwargs)

    def get_stats(self):
        return tuple(sorted(tuple(self._stats.items()), key=lambda t: t[1].runs))

    def total_stats(self):
        total = Stats("Total")
        for name, stats in self.get_stats():
            total.runs += stats.runs
            total.processing_time += stats.processing_time
            total.total_time += stats.total_time
        return total

    def print_stats(self):
        print("\n# STATS for actor '{}'".format(self.name))
        print("Sorted by number of runs. (total time[s]/runs = avg time[s])")
        for name, stats in self.get_stats():
            print(stats)
        print(self.total_stats())

    def add_handler(self, event_name, handler):
        self._stats[event_name] = Stats(event_name)

        super().add_handler(event_name, handler)

    def add_callback(self, event_name, function, message=None, schedule=0):
        if event_name not in self._stats:
            self._stats[event_name] = Stats(event_name)

        super().add_callback(event_name, function, message, schedule)

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

        self.last_updated = now


    class MeasuredEvent(Event):

        def __init__(self, function, message, delay=None):
            super().__init__(function, message, delay=delay)

            self.created_at = time.time()
            self.processing_started_at = None

        def process(self):
            self.processing_started_at = time.time()
            Event.process(self)

