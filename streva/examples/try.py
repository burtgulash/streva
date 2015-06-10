#!/usr/bin/env python3

import logging
import queue
import signal
import threading

from streva.actor import MeasuredMixin, MonitoredMixin, SupervisorMixin, Actor, Stats
from streva.reactor import Reactor, Emperor


class StopProduction(Exception):
    pass


class Producer(MeasuredMixin, MonitoredMixin, Actor):

    def __init__(self, reactor, name):
        super().__init__(reactor, name)
        self.out = self.make_port("out")
        self.count = 1

    def init(self):
        self.add_timeout(self.produce, .00001)

    def produce(self, msg):
        self.out.send(self.count)
        self.count += 1
        self.add_timeout(self.produce, .1)


class Consumer(MeasuredMixin, MonitoredMixin, Actor):

    def __init__(self, reactor, name):
        super().__init__(reactor, name)
        self.add_handler("in", self.on_receive)

    def on_receive(self, msg):
        logging.info("Count is: {}".format(msg))


class Supervisor(SupervisorMixin, Actor):

    def __init__(self, reactor, name):
        super().__init__(reactor, name, timeout_period=1.0, probe_period=4.0)
        self.add_handler("finish", self.finish)
        self.stopped = False

    def error_received(self, err):
        actor, error = err
        logging.exception(error)
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
        # self.stop() TODO
        # End all action here
        self.get_reactor().stop()

        self.print_statistics()


def register_stop_signal(supervisor):
    def signal_stop_handler(sig, frame):
        supervisor.send("finish", None, urgent=True)

    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(sig, signal_stop_handler)


if __name__ == "__main__":
    reactor = Reactor()

    # Define actors
    consumer = Consumer(reactor, "consumer")
    producer = Producer(reactor, "producer")
    producer.connect("out", consumer, "in")

    supervisor = Supervisor(reactor, "supervisor")
    supervisor.supervise(producer)
    supervisor.supervise(consumer)

    # Register keyinterrupt signals to be effective
    register_stop_signal(supervisor)

    # Configure logging
    logging.basicConfig(format="%(levelname)s -- %(message)s", level=logging.INFO)

    emp = Emperor()
    emp.add_reactor(reactor)

    emp.start()
    emp.join()

