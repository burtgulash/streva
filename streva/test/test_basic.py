from streva.actor import MeasuredMixin, SupervisorMixin, Actor
from streva.reactor import Reactor
import threading


MARGINAL_DELAY = .000001
wait = threading.Lock()


class StopProduction(Exception):
    pass


class Producer(Actor):

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


class Consumer(Actor):

    def __init__(self, reactor, name):
        super().__init__(reactor=reactor, name=name)
        self.add_handler("in", self.on_receive)

    def on_receive(self, msg):
        if msg > 100:
            raise StopProduction


class Supervisor(SupervisorMixin, Actor):

    def __init__(self, reactor, name):
        super().__init__(reactor=reactor, name=name)
        self.stopped = False

    def error_received(self, err):
        errored_event, error = err
        if isinstance(error, StopProduction):
            self.stop_supervised()
            self.stop()
            self.stopped = True

            wait.release()



def test_count_to_100():
    reactor = Reactor()

    # Define actors
    producer = Producer(reactor, "producer")
    consumer = Consumer(reactor, "consumer")
    supervisor = Supervisor(reactor, "supervisor")

    producer.connect("out", consumer, "in")
    supervisor.supervise(producer)
    supervisor.supervise(consumer)

    wait.acquire()
    reactor.start()

    wait.acquire()
    reactor.stop()

    assert True

