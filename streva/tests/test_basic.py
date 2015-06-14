import queue

import streva.reactor
from streva.actor import Actor, Timer, Supervisor, Supervised, Process
from streva.reactor import LoopReactor, TimedReactor, Emperor


MARGINAL_DELAY = .000001


class StopProduction(Exception):
    pass


class Producer(Actor):

    def __init__(self, timer, to):
        super().__init__()
        self.to = to
        self.count = 1

        self.timer = timer.timer_proxy()
        self.timer.send((self, "produce", MARGINAL_DELAY))

    @handler_for("produce")
    def produce(self, msg):
        self.timer.send((self, "produce", MARGINAL_DELAY))
        self.to.send("receive", self.count)
        self.count += 1


class Consumer(Actor):

    @handler_for("receive")
    def on_receive(self, msg):
        if msg > 100:
            raise StopProduction


class Supervisor(Supervisor, Actor):

    def __init__(self, timer, children=[]):
        super().__init__(timer=timer)
        self.emperor = None
        for child in children:
            self.supervise(child)

    def set_emperor(self, emperor):
        self.emperor = emperor

    def error_received(self, error_context):
        error = error_context.get_exception()
        if isinstance(error, StopProduction):
            self.stop_children()

    def all_stopped(self):
        self.emperor.stop()
        self.stop()



def test_count_to_100():
    # Define processes
    timer = Timer()
    timer.timer = timer.make_port("timer")

    consumer = Consumer()
    producer = Producer(timer, to=consumer)
    supervisor = Supervisor(timer.timer, children=[producer, consumer])

    # Define reactors
    loop = LoopReactor(processes=[consumer, producer, supervisor])
    timer_loop = TimedReactor(processes=[timer])

    emp = Emperor(children=[loop, timer_loop])
    supervisor.set_emperor(emp)

    supervisor.start()
    timer.start()

    emp.start()
    emp.join()

    assert True

