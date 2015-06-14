#!/usr/bin/env python3

import logging
import queue
import signal
import threading

from streva.actor import Timer, Measured, Monitored, Supervisor, Actor, Stats
from streva.reactor import Reactor, LoopReactor, TimedReactor, Emperor



class Producer(Measured, Monitored, Actor):

    def __init__(self, timer):
        super().__init__()
        self.out = self.make_port("out")
        self.count = 1

        self.timer = timer.register_timer(self)
        self.timer.send((self, "produce", .00001))

    @handler_for("produce")
    def produce(self, msg):
        self.out.send(self.count)
        self.count += 1
        self.timer.send((self, "produce", .01))


class Consumer(Measured, Monitored, Actor):

    @handler_for("in")
    def on_receive(self, msg):
        logging.info("Count is: {}".format(msg))



class Supervisor(Supervisor):

    def __init__(self, timer, children=[]):
        super().__init__(timer, children=children, timeout_period=1.0, probe_period=4.0)
        self.stopped = False
        self.emperor = None

    def set_emperor(self, emperor):
        self.emperor = emperor

    def error_received(self, error_context):
        error = error_context.get_exception()
        logging.error(str(error_context))
        self.finish(None)

    @handler_for("finish")
    def finish(self, _):
        if not self.stopped:
            self.stopped = True
            self.stop_children()

    def terminate(self):
        self.print_statistics()
        self.emperor.stop()

    def print_statistics(self):
        bottomline = Stats("TOTAL RUN STATISTICS")
        for actor in self.get_supervised():
            try:
                actor.print_stats()
                bottomline.add(actor.get_total_stats())
            except AttributeError:
                # Some actors need not have get_stats() because they are
                # not MeasuredActor
                pass

        print("\n--------------------------------------------------")
        print(bottomline)




# Register keyinterrupt signals to be effective
def register_stop_signal(supervisor, emperor):
    supervisor.set_emperor(emperor)

    def signal_stop_handler(sig, frame):
        supervisor.send("finish", None)

    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(sig, signal_stop_handler)


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(format="%(levelname)s -- %(message)s", level=logging.INFO)

    # Define actors
    timer = Timer()
    consumer = Consumer()
    producer = Producer(timer)
    supervisor = Supervisor(timer, children=[consumer, producer])

    producer.connect("out", consumer, "in")
    supervisor.start()
    timer.start()

    # Define reactors
    loop = LoopReactor(actors=[supervisor, consumer, producer])
    timer_loop = TimedReactor(actors=[timer])

    emp = Emperor(children=[loop, timer_loop])
    register_stop_signal(supervisor, emp)

    emp.start()
    emp.join()

