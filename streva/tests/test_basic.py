import queue

import streva.reactor
from streva.actor import DelayableMixin, TimerMixin, MeasuredMixin, MonitoredMixin, SupervisorMixin, Actor
from streva.reactor import Loop, TimedLoop, Emperor


MARGINAL_DELAY = .000001


class StopProduction(Exception):
    pass


class Producer(MonitoredMixin, DelayableMixin, Actor):

    def __init__(self, loop, name):
        super().__init__(loop, name)
        self.add_handler("produce", self.produce)
        self.out = self.make_port("out")
        self.count = 1

    def init(self):
        self.delay("produce", MARGINAL_DELAY)

    def produce(self, msg):
        self.delay("produce", MARGINAL_DELAY)
        self.out.send(self.count)
        self.count += 1


class Consumer(MonitoredMixin, Actor):

    def __init__(self, loop, name):
        super().__init__(loop, name)
        self.add_handler("in", self.on_receive)

    def on_receive(self, msg):
        if msg > 100:
            raise StopProduction


class Supervisor(SupervisorMixin, TimerMixin, Actor):

    def __init__(self, loop, name, emperor):
        super().__init__(loop, name, timeout_period=.1, probe_period=.5)
        self.stopped = False
        self.emperor = emperor

    def error_received(self, error_context):
        error = error_context.get_exception()
        if isinstance(error, StopProduction) and not self.stopped:
            self.stopped = True
            self.stop_children()

    def all_stopped(self, _):
        self.emperor.stop()
        self.stop()



def test_count_to_100():
    loop = Loop()
    timer_loop = TimedLoop()

    emp = Emperor()
    emp.add_loop(loop)
    emp.add_loop(timer_loop)

    # Define actors
    producer = Producer(loop, "producer")
    consumer = Consumer(loop, "consumer")
    supervisor = Supervisor(timer_loop, "supervisor", emp)

    producer.connect("out", consumer, "in")
    supervisor.supervise(producer)
    supervisor.supervise(consumer)

    producer.connect_timer(supervisor)
    supervisor.connect_timer(supervisor)


    emp.start()
    emp.join()

    assert True

