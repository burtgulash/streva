#!/usr/bin/env python3

import logging
import queue
import signal
import threading

from streva.actor import MeasuredMixin, MonitoredMixin, SupervisorMixin, Actor, Stats
from streva.reactor import TimedLoop, Emperor


class StopProduction(Exception):
    pass


class Producer(MeasuredMixin, MonitoredMixin, Actor):

    def __init__(self, loop, name):
        super().__init__(loop, name)
        self.out = self.make_port("out")
        self.count = 1

    def init(self):
        self.add_timeout(self.produce, .00001)

    def produce(self, msg):
        self.out.send(self.count)
        self.count += 1
        self.add_timeout(self.produce, .01)


class Consumer(MeasuredMixin, MonitoredMixin, Actor):

    def __init__(self, loop, name):
        super().__init__(loop, name)
        self.add_handler("in", self.on_receive)

    def on_receive(self, msg):
        logging.info("Count is: {}".format(msg))


class Supervisor(SupervisorMixin, Actor):

    def __init__(self, loop, name):
        super().__init__(loop, name, timeout_period=1.0, probe_period=4.0)
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

    def finish(self, _):
        if not self.stopped:
            self.stopped = True
            self.stop_children()

    def all_stopped(self, _):
        self.stop()
        # End all action here
        self.get_loop().stop()

        self.print_statistics()


def register_stop_signal(supervisor):
    def signal_stop_handler(sig, frame):
        supervisor.send("finish", None, urgent=True)

    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(sig, signal_stop_handler)


if __name__ == "__main__":
    loop = TimedLoop()

    # Define actors
    consumer = Consumer(loop, "consumer")
    producer = Producer(loop, "producer")
    producer.connect("out", consumer, "in")

    supervisor = Supervisor(loop, "supervisor")
    supervisor.supervise(producer)
    supervisor.supervise(consumer)

    # Register keyinterrupt signals to be effective
    register_stop_signal(supervisor)

    # Configure logging
    logging.basicConfig(format="%(levelname)s -- %(message)s", level=logging.INFO)

    emp = Emperor()
    emp.add_loop(loop)

    emp.start()
    emp.join()

