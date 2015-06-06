import streva.actor
import streva.reactor
import threading


MARGINAL_DELAY = .000001
wait = threading.Lock()


class StopProduction(Exception):
    pass


class Producer(streva.actor.Actor):

    def __init__(self, reactor, name):
        super().__init__(reactor=reactor, name=name)
        self.out = self.make_port("out")
        self.add_handler("stop", self.on_stop)
        self.count = 1

    def init(self, _):
        self.add_timeout(self.produce, MARGINAL_DELAY)

    def on_stop(self, msg):
        self.stop()

    def produce(self, msg):
        self.add_timeout(self.produce, MARGINAL_DELAY)
        self.out.send(self.count)
        self.count += 1


class Consumer(streva.actor.Actor):

    def __init__(self, reactor, name):
        super().__init__(reactor=reactor, name=name)
        self.add_handler("in", self.on_receive)
        self.add_handler("stop", self.on_stop)

    def on_stop(self, msg):
        self.stop()

    def on_receive(self, msg):
        if msg > 100:
            raise StopProduction


class Supervisor(streva.actor.SupervisorMixin):

    def error_received(self, err):
        errored_event, error = err
        if isinstance(error, StopProduction):
            self.broadcast_supervised("stop", None)
            wait.release()



def test_count_to_100():
    reactor = streva.reactor.Reactor()

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

