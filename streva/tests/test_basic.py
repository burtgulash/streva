import queue

import streva.reactor
from streva.actor import MeasuredMixin, MonitoredMixin, SupervisorMixin, Actor
from streva.reactor import Reactor


MARGINAL_DELAY = .000001


class StopProduction(Exception):
    pass


class Producer(MonitoredMixin, Actor):

    def __init__(self, reactor, name):
        super().__init__(reactor=reactor, name=name)
        self.out = self.make_port("out")
        self.count = 1

    def init(self, _):
        self.add_timeout(self.produce, MARGINAL_DELAY)

    def produce(self, msg):
        self.add_timeout(self.produce, MARGINAL_DELAY)
        self.out.send(self.count)
        self.count += 1


class Consumer(MonitoredMixin, Actor):

    def __init__(self, reactor, name):
        super().__init__(reactor=reactor, name=name)
        self.add_handler("in", self.on_receive)

    def on_receive(self, msg):
        if msg > 100:
            raise StopProduction


class Supervisor(SupervisorMixin, Actor):

    def __init__(self, reactor, name):
        super().__init__(reactor=reactor, name=name, timeout_period=.1, probe_period=.5)
        self.stopped = False

    def error_received(self, err):
        errored_event, error = err
        if isinstance(error, StopProduction):
            self.stopped = True
            self.stop_children()

    def all_stopped(self, _):
        self.stop()
        self._reactor.stop()



def test_count_to_100():
    done = queue.Queue()
    reactor = Reactor(done)

    # Define actors
    producer = Producer(reactor, "producer")
    consumer = Consumer(reactor, "consumer")
    supervisor = Supervisor(reactor, "supervisor")

    producer.connect("out", consumer, "in")
    supervisor.supervise(producer)
    supervisor.supervise(consumer)

    reactor.start()

    _, sig = done.get()
    try:
        raise sig
    except streva.reactor.Done:
        pass

    assert True

