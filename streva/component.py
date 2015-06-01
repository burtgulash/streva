
class Component:
    """ Component is a logical construct sitting upon Reactor, which it uses
    as its backend.

    Components can route outgoing messages through Ports. Port is a publisher
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

    def make_port(self, name):
        port = self._Port(name)
        self._ports[name] = port
        return port

    def connect(self, port_name, to_component, to_event_name):
        port = self._ports[port_name]
        port._targets.append((to_component, to_event_name))

    def send(self, event_name, message):
        handler = self._handlers[event_name]
        self.call(lambda: handler(message))

    def call(self, function, delay=None):
        event = self._reactor.schedule(function, delay=delay)
        self._events_planned[id(event)] = event


    def add_handler(self, event_name, handler):
        self._handlers[event_name] = handler

    def on_start(self, message):
        pass

    def on_end(self, message):
        pass


    class _Port:
        """ Port is a named set of components to all of which an outbound
        message will be sent through this port. Port implements pubsub routing.
        """

        def __init__(self, name):
            self.name = name
            self._targets = []

        def send(self, message):
            """ Send message to all connected components through this pubsub port.
            """
            for target_component, event_name in self._targets:
                target_component.send(event_name, message)

