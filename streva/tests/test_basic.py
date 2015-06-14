import queue

import streva.reactor
from streva.actor import Timer, Measured, Monitored, Supervisor, Actor
from streva.reactor import LoopReactor, TimedReactor, Emperor


MARGINAL_DELAY = .000001


class StopProduction(Exception):
    pass


class Producer(Monitored, Actor):

    def __init__(self, name, timer, to):
        super().__init__(name)
        self.to = to
        self.count = 1

        self.timer = timer.register_timer(self)
        self.timer.send((self, "produce", MARGINAL_DELAY))

    @handler_for("produce")
    def produce(self, msg):
        self.timer.send((self, "produce", MARGINAL_DELAY))
        self.to.send("receive", self.count)
        self.count += 1


class Consumer(Monitored, Actor):

    @handler_for("receive")
    def on_receive(self, msg):
        if msg > 100:
            raise StopProduction


class Supervisor(Supervisor, Actor):

    def __init__(self, name, timer, emperor, children=[]):
        super().__init__(name, timer, children=children, timeout_period=.1, probe_period=.5)
        self.emperor = emperor

    def error_received(self, error_context):
        error = error_context.get_exception()
        if isinstance(error, StopProduction):
            self.stop_children()

    def all_stopped(self, _):
        self.emperor.stop()
        self.stop()



def test_count_to_100():
    emp = Emperor()

    # Define actors
    timer = Timer("timer")
    consumer = Consumer("consumer")
    producer = Producer("producer", timer, to=consumer)
    supervisor = Supervisor("supervisor", timer, emp, children=[producer, consumer])

    # Register processes within reactors
    loop = LoopReactor(actors=[consumer, producer, supervisor])
    timer_loop = TimedReactor(actors=[timer])

    supervisor.start()
    timer.start()

    emp.add_reactor(loop)
    emp.add_reactor(timer_loop)

    emp.start()
    emp.join()

    assert True

