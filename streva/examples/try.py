#!/usr/bin/env python3

import logging
import queue
import signal
import threading

from streva.actor import Delayable, Timer, Measured, Monitored, Supervisor, Actor, Stats
from streva.reactor import Reactor, LoopReactor, TimedReactor, Emperor


class StopProduction(Exception):
    pass


class Producer(Measured, Monitored, Delayable, Actor):

    def __init__(self, name):
        super().__init__(name)
        self.out = self.make_port("out")
        self.count = 1

        self.delay("produce", .00001)

    @handler_for("produce")
    def produce(self, msg):
        self.out.send(self.count)
        self.count += 1
        self.delay("produce", .01)


class Consumer(Measured, Monitored, Actor):

    @handler_for("in")
    def on_receive(self, msg):
        logging.info("Count is: {}".format(msg))


class Manager(Supervisor, Timer, Actor):

    def __init__(self, name, children=[]):
        super().__init__(name, children=children, timeout_period=1.0, probe_period=4.0)
        self.stopped = False
        self.emperor = None

    def set_emperor(self, emperor):
        self.emperor = emperor

    def error_received(self, error_context):
        error = error_context.get_exception()
        logging.error(str(error_context))
        self.finish(None)

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

    @handler_for("finish")
    def finish(self, _):
        if not self.stopped:
            self.stopped = True
            self.stop_children()

    def terminate(self):
        self.print_statistics()
        self.emperor.stop()



# Register keyinterrupt signals to be effective
def register_stop_signal(manager, emperor):
    manager.set_emperor(emperor)

    def signal_stop_handler(sig, frame):
        manager.send("finish", None)

    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(sig, signal_stop_handler)


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(format="%(levelname)s -- %(message)s", level=logging.INFO)

    # Define actors
    consumer = Consumer("consumer")
    producer = Producer("producer")
    manager = Manager("manager", children=[consumer, producer])

    producer.connect("out", consumer, "in")

    producer.connect_timer(manager)
    manager.connect_timer(manager)
    manager.start()

    # Define reactors
    loop = LoopReactor(actors=[consumer, producer])
    timer_loop = TimedReactor(actors=[manager])

    emp = Emperor(children=[loop, timer_loop])
    register_stop_signal(manager, emp)
    emp.start()
    emp.join()

