import logging
import time
import traceback
from .reactor import Event



class ContextException(Exception):

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



class ActorBase:
    """ Actor is a logical construct sitting upon Reactor, which it uses
    as its backend.

    Actors can route outgoing messages through Ports. Port is a publisher
    mechanism, which sends messages to its subscribers.
    """

    def __init__(self, reactor, name):
        self.name = name

        self._reactor = reactor

        self._events_planned = {}
        self._handlers = {}
        self._ports = {}

        # Set up reactor's lifecycle observation
        self._reactor.add_observer("start", self.init)
        self._reactor.add_observer("end", self.terminate)

        # Listen on lifecycle events
        self.add_handler("start", self.init)
        self.add_handler("end", self.terminate)

    # Actor lifecycle methods
    def init(self, message):
        pass

    def terminate(self, message):
        pass

    def on_error(self, error):
        raise error


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


    # Event and error handling
    def _handle_error(self, error_message):
        errored_event, error = error_message
        if errored_event in self._events_planned:
            event_name = self._events_planned[errored_event]
            del self._events_planned[errored_event]

            error = ContextException(self.name, event_name, errored_event.message, error)
            self.on_error(error)

    def _on_event_processed(self, event):
        assert event in self._events_planned
        del self._events_planned[event]


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


    # Scheduling and sending methods
    def send(self, event_name, message):
        handler = self._handlers[event_name]
        self._schedule(handler, message, event_name)

    def add_timeout(self, function, delay, message=None):
        self._schedule(function, message, "timeout", delay)

    def _schedule(self, function, message, event_name, delay=None):
        event = Event(function, message, delay=delay)
        event.ok(self._on_event_processed)
        event.err(self._handle_error)

        self._events_planned[event] = event_name
        self._reactor.schedule(event)


class MonitoredMixin(ActorBase):
    """ Allows the Actor object to be monitored by Supervisors.
    """

    def __init__(self, reactor, **kwargs):
        super().__init__(reactor=reactor, **kwargs)
        self.add_handler("_ping", self._ping)
        self.error_out = self.make_port("_error")

    def _ping(self, msg):
        sender = msg
        sender.send("_pong", self)

    def _handle_error(self, error_message):
        super()._handle_error(error_message)
        self.error_out.send(error_message)

    def on_error(self, err):
        """ Delegate the error to attached supervisor. Therefore on_error need
        not to be handled. 
        """
        pass


class Actor(MonitoredMixin, ActorBase):

    def __init__(self, reactor, name, **kwargs):
        super().__init__(reactor=reactor, name=name, **kwargs)



class SupervisorMixin(ActorBase):

    def __init__(self, reactor, name, probe_period=60, timeout_period=10, **kwargs):
        self._supervised_actors = set()
        self._ping_questions = {}

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

        super().__init__(reactor=reactor, name=name, **kwargs)
        self.add_handler("_error", self.error_received)
        self.add_handler("_pong", self._receive_pong)

    def supervise(self, actor):
        self._supervised_actors.add(actor)
        actor.connect("_error", self, "_error")

    def broadcast_supervised(self, event_name, msg):
        for actor in self._supervised_actors:
            actor.send(event_name, msg)

    def get_supervised(self):
        return list(self._supervised_actors)

    # Not responding and error handlers
    def not_responding(self, actor):
        name = actor.name or str(id(actor))
        logging.error("Actor '{}' hasn't responded in {} seconds!".format(name,
                                                                    self._failure_timeout_period))

    def error_received(self, error_message):
        errored_event, error = error_message
        raise error


    # Supervisor processes
    def init(self, msg):
        super().init(msg)

        def probe(_):
            # Reset ping questions
            self._ping_questions = set()

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
"""Processing (processing time [s] / runs = avg [s]):  {:.4f} / {} = {:.4f}
Total      (total time      [s] / runs = avg [s]):  {:.4f} / {} = {:.4f}
""".format(self.processing_time, self.runs, avg_processing,
           self.total_time, self.runs, avg_total)



class MeasuredMixin(ActorBase):

    def __init__(self, reactor, **kwargs):
        self._stats = {}
        self.last_updated = time.time()
        super().__init__(reactor=reactor, **kwargs)

    def get_stats(self):
        return self._stats

    def print_stats(self):
        for x, y in self._stats.items():
            print("{}\n{}".format(x, y))

    def add_handler(self, event_name, handler):
        self._stats[event_name] = Stats(event_name)

        super().add_handler(event_name, handler)

    def _on_event_processed(self, event):
        event_name = self._events_planned[event]
        super()._on_event_processed(event)

        self._collect_statistics(event_name, event)

    def _handle_error(self, error_message):
        errored_event, _ = error_message
        event_name = self._events_planned[errored_event]
        super()._handle_error(error_message)

        self._collect_statistics(event_name, errored_event)

    def _schedule(self, function, message, event_name, delay=None):
        event = self.MeasuredEvent(function, message, delay=None)
        event.ok(self._on_event_processed)
        event.err(self._handle_error)

        self._events_planned[event] = event_name
        self._reactor.schedule(event)

    def _collect_statistics(self, event_name, event):
        stats = self._stats[event_name]
        now = time.time()

        stats.runs += 1
        stats.processing_time += now - event.processing_started_at
        stats.total_time += now - event.created_at

        self.last_updated = now


    class MeasuredEvent(Event):

        __slots__ = "created_at", "processing_started_at"

        def __init__(self, function, message, delay=None):
            Event.__init__(self, function, message, delay=delay)

            self.created_at = time.time()

        def process(self):
            self.processing_started_at = time.time()
            Event.process(self)

