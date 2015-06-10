import queue

import streva.reactor
from streva.actor import MeasuredMixin, MonitoredMixin, SupervisorMixin, Actor
from streva.reactor import TimedLoop, Emperor


MARGINAL_DELAY = .000001


class StopProduction(Exception):
    pass


class Producer(MonitoredMixin, Actor):

    def __init__(self, loop, name):
        super().__init__(loop, name)
        self.out = self.make_port("out")
        self.count = 1

    def init(self):
        self.add_timeout(self.produce, MARGINAL_DELAY)

    def produce(self, msg):
        self.add_timeout(self.produce, MARGINAL_DELAY)
        self.out.send(self.count)
        self.count += 1


class Consumer(MonitoredMixin, Actor):

    def __init__(self, loop, name):
        super().__init__(loop, name)
        self.add_handler("in", self.on_receive)

    def on_receive(self, msg):
        if msg > 100:
            raise StopProduction


class Supervisor(SupervisorMixin, Actor):

    def __init__(self, loop, name):
        super().__init__(loop, name, timeout_period=.1, probe_period=.5)
        self.stopped = False

    def error_received(self, error_context):
        error = error_context.get_exception()
        if isinstance(error, StopProduction) and not self.stopped:
            self.stopped = True
            self.stop_children()

    def all_stopped(self, _):
        self.stop()
        self.get_loop().stop()



def test_count_to_100():
    loop = TimedLoop()

    # Define actors
    producer = Producer(loop, "producer")
    consumer = Consumer(loop, "consumer")
    supervisor = Supervisor(loop, "supervisor")

    producer.connect("out", consumer, "in")
    supervisor.supervise(producer)
    supervisor.supervise(consumer)

    emp = Emperor()
    emp.add_loop(loop)

    emp.start()
    emp.join()

    assert True

