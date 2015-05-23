
class Component:
    """ Component is a logical construct sitting upon Reactor, which it uses
    as its backend.

    Components can route outgoing messages through Ports. Port is a publisher
    mechanism, which sends messages to its subscribers.
    """

    def __init__(self, reactor):
        self._reactor = reactor
        self._ports = {}

    def make_port(self, name):
        port = self._Port(name)
        self._ports[name] = port
        return port

    def connect(self, port_name, to_component, to_event_name):
        port = self._ports[port_name]
        port._targets.append((to_component, to_event_name))

    def subscribe(self, event_name, source_component, source_port_name):
        source_component.connect(source_port_name, self, event_name)

    def send(self, event_name, message):
        self._reactor.send(self._unique_event_id(event_name), message)

    def add_handler(self, event_name, handler, reactor_event=False):
        if not reactor_event:
            event_name = self._unique_event_id(event_name)
        self._reactor.add_handler(event_name, handler)

    def call_later(self, delay, callback, *args, **kwargs):
        self._reactor.call_later(delay, callback, *args, **kwargs)

    def _unique_event_id(self, event_name):
        """ Make event_name unique by combining unique element of this
        component with event_name. 
        """
        return str(id(self)) + event_name


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



class Stats:

    def __init__(self):
        self.operation_stats = {}
        self.queue_size = 0

    def register_operation_stats(self, operation_name):
        self.operation_stats[operation_name] = Stats.OperationStats()

    def update_running_stats(self, operation_name, running_time, number_of_runs):
        op_stats = self.operation_stats[operation_name]
        op_stats.runs += number_of_runs
        op_stats.total_time += running_time

    # TODO dump stats into serialized form

    class OperationStats:

        def __init__(self):
            self.runs = 0
            self.total_time = 0

