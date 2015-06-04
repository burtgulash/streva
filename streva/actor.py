import time
from .reactor import Event



class ContextException(Exception):

    def __init__(self, actor_name, event_name, message, err):
        super().__init__(self, message)

        self.actor_name = actor_name or "[actor]"
        self.event_name = event_name
        self.message = message
        self.err = err

    def __str__(self):
        return "{}.{}  <-  '{}'\n{}".format(self.actor_name,
                                        self.event_name,
                                        str(self.message)[:30] if self.message else "message",
                                        self.err)


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

    def __init__(self, reactor, name=None):
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
    def queue_size(self):
        # note: queue size includes both tasks and timeouts
        return len(self._events_planned)

    def flush(self):
        flushed_messages = []

        for event in self._events_planned:
            event.deactivate()
            flushed_messages.append(event.message)

        self._events_planned = {}
        return flushed_messages


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




class MeasuredActor(Actor):


    def __init__(self, reactor, name=None):
        Actor.__init__(self, reactor, name=name)

        self._stats = {}
        for event_name in self._handlers:
            self._stats[event_name] = self.Stats(event_name)

    def _on_event_processed(self, event):
        event_name = self._events_planned[event]
        Actor._on_event_processed(self, event)

        self._collect_statistics(event_name, event)

    def _handle_error(self, error_message):
        errored_event, _ = error_message
        event_name = self._events_planned[errored_event]
        Actor._handle_error(self, error_message)

        self._collect_statistics(event_name, errored_event)
    
    def _schedule(function, message, event_name, delay=None):
        event = self.MeasuredEvent(function, message, event_name, delay=None)
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


    class MeasuredEvent(Event):

        __slots__ = "created_at", "processing_started_at"
        
        def __init__(self, function, message, delay=None):
            Event.__init__(self, function, message, delay=delay)

            self.created_at = self.now()

        def process(self):
            self.processing_started_at = time.time()
            Event.process(self)


    class Stats:

        def __init__(self, event_name):
            self.event_name = event_name
            self.runs = 0
            self.processing_time = 0
            self.total_time
