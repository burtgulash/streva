
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

        # Setup reactor's lifecycle observation
        self._reactor.add_observer(self, "start")
        self._reactor.add_observer(self, "end")
        self.add_handler("start", self.on_start)
        self.add_handler("end", self.on_end)

    # Lifecycle methods
    def on_start(self, message):
        pass

    def on_end(self, message):
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

    # Scheduling and sending methods
    def send(self, event_name, message):
        handler = self._handlers[event_name]
        self._schedule(lambda: handler(message))

    def add_timeout(self, callback, delay):
        self._schedule(callback, delay=delay)

    def _schedule(self, callback, delay=None):
        event = self._reactor.schedule(callback, delay=delay)
        self._events_planned[id(event)] = event


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


class SupervisedActor(Actor):

    def __init__(self, reactor):
        Actor.__init__(self, reactor)
        self._error_ = self.make_port("_error_")

    def _make_safe_callback(self, callback, event_name, message):
        def safe_cb():
            try:
                callback()
            except ValueError as err:
                error_message = self.ErrorContext(event_name, message, err)
                self._error_.send(error_message)

        return safe_cb

    def send(self, event_name, message):
        handler = self._handlers[event_name]
        cb = self._make_safe_callback(lambda: handler(message), event_name, message)
        self._schedule(cb)

    def add_timeout(self, callback, delay):
        cb = self._make_safe_callback(callback, "timeout", None)
        self._schedule(cb, delay=delay)


    class ErrorContext:

        def __init__(self, event_name, message, err):
            self.event_name = event_name
            self.message = message
            self.err = err

