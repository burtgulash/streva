
class ErrorContext:

    def __init__(self, actor_name, event_name, message, err):
        self.actor_name = actor_name or "[actor]"
        self.event_name = event_name
        self.message = message
        self.err = err

    def __str__(self):
        return "{}.{}  <-  '{}'\n{}".format(self.actor_name,
                                        self.event_name,
                                        str(self.message)[:30] if self.message else "message",
                                        self.err)


class Actor:
    """ Actor is a logical construct sitting upon Reactor, which it uses
    as its backend.

    Actors can route outgoing messages through Ports. Port is a publisher
    mechanism, which sends messages to its subscribers.
    """

    def __init__(self, reactor, name=None):
        self.name = name

        self._reactor = reactor

        self._events_planned = set()
        self._handlers = {}
        self._ports = {}

        # Set up reactor's lifecycle observation
        self._reactor.add_observer("start", self.init)
        self._reactor.add_observer("end", self.terminate)
        self._reactor.add_observer("task_processed", self._on_event_processed)
        self._reactor.add_observer("timeout_processed", self._on_event_processed)
        self._reactor.add_observer("error", self._handle_error)

        # Listen on lifecycle events
        self.add_handler("start", self.init)
        self.add_handler("end", self.terminate)

    # Actor lifecycle methods
    def init(self, message):
        pass

    def terminate(self, message):
        pass

    def on_error(self, error):
        pass


    # Actor construction and setup methods
    def make_port(self, name):
        port = self._Port(name)
        self._ports[name] = port
        return port

    def connect(self, port_name, to_actor, to_event_name):
        port = self._ports[port_name]
        port._targets.append((to_actor, to_event_name))

    def add_handler(self, event_name, handler):
        self._handlers[event_name] = handler


    # Event and error handling
    def _handle_error(self, error_message):
        errored_event, error = error_message
        if errored_event in self._events_planned:
            self._events_planned.remove(errored_event)
            self.on_error(error)

    def _on_event_processed(self, event):
        if event in self._events_planned:
            self._events_planned.remove(event)


    # Actor diagnostic and control methods
    def queue_size(self):
        return len(self._events_planned)

    def flush(self):
        for event in self._events_planned:
            event.callback.deactivate()
        self._events_planned = set()


    # Scheduling and sending methods
    def send(self, event_name, message):
        handler = self._handlers[event_name]
        self._schedule(lambda: handler(message))

    def add_timeout(self, callback, delay):
        self._schedule(callback, delay=delay)

    def _schedule(self, callback, delay=None):
        event = self._reactor.schedule(callback, delay=delay)
        self._events_planned.add(event)


    class _Port:
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

