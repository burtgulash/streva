import streva
import threading


MARGINAL_DELAY = .000001
wait = threading.Lock()


class StopProduction(Exception):
    pass


class Producer(streva.actor.Actor):

    def __init__(self, reactor, name):
        super().__init__(reactor=reactor, name=name)
        self.out = self.make_port("out")
        self.count = 1

    def init(self, _):
        self.add_timeout(produce, MARGINAL_DELAY)

    def produce(self, msg):
        self.add_timeout(produce, MARGINAL_DELAY)
        self.out.send(self.count)
        self.count += 1


class Consumer(streva.actor.Actor):

    def __init__(self, reactor, name):
        super().__init__(reactor=reactor, name=name)
        self.add_handler("in", self.on_receive)

    def on_receive(self, msg):
        if msg > 100:
            raise StopProduction

    def on_error(self, err):
        try:
            pass
        except StopProduction:
            wait.release()



def test_count_to_100():
    reactor = streva.reactor.Reactor()
    producer = Producer(reactor, "producer")
    consumer = Producer(reactor, "consumer")

    wait.acquire()
    reactor.start()

    wait.acquire()
    reactor.stop()

    assert True
