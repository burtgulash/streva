import queue

import streva.reactor
from streva.actor import DelayableMixin, TimerMixin, MeasuredMixin, MonitoredMixin, SupervisorMixin, Actor
from streva.reactor import LoopReactor, TimedReactor, Emperor


MARGINAL_DELAY = .000001


class StopProduction(Exception):
    pass


class Producer(MonitoredMixin, DelayableMixin, Actor):

    def __init__(self, name, to):
        super().__init__(name)
        self.to = to
        self.count = 1
        self.delay("produce", MARGINAL_DELAY)

    @handler_for("produce")
    def produce(self, msg):
        self.delay("produce", MARGINAL_DELAY)
        self.to.send("receive", self.count)
        self.count += 1


class Consumer(MonitoredMixin, Actor):

    @handler_for("receive")
    def on_receive(self, msg):
        if msg > 100:
            raise StopProduction


class Supervisor(SupervisorMixin, TimerMixin, Actor):

    def __init__(self, name, emperor, children=[]):
        super().__init__(name, children=children, timeout_period=.1, probe_period=.5)
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
    loop = LoopReactor()
    timer_loop = TimedReactor()

    emp = Emperor()

    # Define actors
    consumer = Consumer("consumer")
    producer = Producer("producer", to=consumer)
    supervisor = Supervisor("supervisor", emp, children=[producer, consumer])

    producer.connect_timer(supervisor)
    supervisor.connect_timer(supervisor)

    # Register processes within reactors
    producer.set_reactor(loop)
    consumer.set_reactor(loop)
    supervisor.set_reactor(timer_loop)

    supervisor.start()

    emp.add_reactor(loop)
    emp.add_reactor(timer_loop)

    emp.start()
    emp.join()

    assert True

