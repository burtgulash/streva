#!/usr/bin/env python3

import logging
import queue
import signal
import threading

from streva.actor import DelayableMixin, TimerMixin, MeasuredMixin, MonitoredMixin, SupervisorMixin, Actor, Stats
from streva.reactor import Reactor, LoopReactor, TimedReactor, Emperor


class StopProduction(Exception):
    pass


class Producer(MeasuredMixin, MonitoredMixin, DelayableMixin, Actor):

    def __init__(self, name):
        super().__init__(name)
        self.out = self.add_handler("produce", self.produce)
        self.out = self.make_port("out")

        self.count = 1
        self.delay("produce", .00001)

    def produce(self, msg):
        self.out.send(self.count)
        self.count += 1
        self.delay("produce", .01)


class Consumer(MeasuredMixin, MonitoredMixin, Actor):

    def __init__(self, name):
        super().__init__(name)
        self.add_handler("in", self.on_receive)

    def on_receive(self, msg):
        logging.info("Count is: {}".format(msg))


class Supervisor(SupervisorMixin, TimerMixin, Actor):

    def __init__(self, name, children=[]):
        super().__init__(name, children=children, timeout_period=1.0, probe_period=4.0)
        self.add_handler("finish", self.finish)
        self.stopped = False

    def error_received(self, error_context):
        error = error_context.error
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

    def finish(self, emperor):
        self.emperor = emperor
        if not self.stopped:
            self.stopped = True
            self.stop_children()

    def terminate(self):
        self.print_statistics()
        self.emperor.stop()



def register_stop_signal(supervisor, emperor):
    def signal_stop_handler(sig, frame):
        supervisor.send("finish", emperor)

    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(sig, signal_stop_handler)


if __name__ == "__main__":
    loop = LoopReactor()
    timer_loop = TimedReactor()

    emp = Emperor(children=[loop, timer_loop])
    emp.start()


    # Define actors
    consumer = Consumer("consumer")
    producer = Producer("producer")
    supervisor = Supervisor("supervisor", children=[consumer, producer])

    producer.connect("out", consumer, "in")

    producer.connect_timer(supervisor)
    supervisor.connect_timer(supervisor)

    # Register keyinterrupt signals to be effective
    register_stop_signal(supervisor, emp)

    # Configure logging
    logging.basicConfig(format="%(levelname)s -- %(message)s", level=logging.INFO)



    consumer.set_reactor(loop)
    producer.set_reactor(loop)
    supervisor.set_reactor(timer_loop)

    supervisor.start()

    emp.join()

