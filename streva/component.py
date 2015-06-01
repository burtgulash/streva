
class Component:
    """ Component is a logical construct sitting upon Reactor, which it uses
    as its backend.

    Components can route outgoing messages through Ports. Port is a publisher
    mechanism, which sends messages to its subscribers.
    """

    def __init__(self, reactor, name=None):
        self.name = name

        self._reactor = reactor

        # Setup reactor's lifecycle observation
        self._reactor.add_observer(self, "start")
        self._reactor.add_observer(self, "end")
        self._add_handler("start", on_start)
        self._add_handler("end", on_end)

        self._events_planned = {}
        self._ports = {}

    def make_port(self, name):
        port = self._Port(name)
        self._ports[name] = port
        return port

    def connect(self, port_name, to_component, to_event_name):
        port = self._ports[port_name]
        port._targets.append((to_component, to_event_name))

    def send(self, event_name, message, delay=None):
        handler = self._handlers[event_name]

        event = self._reactor.schedule(lambda: handler(message), delay=delay)
        self._events_planned.add(event)

    def add_handler(self, event_name, handler):
        self._handlers[event_name] = handler

    def on_start(self, message):
        raise NotImplementedError

    def on_end(self, message):
        raise NotImplementedError


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

