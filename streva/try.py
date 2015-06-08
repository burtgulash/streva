#!/usr/bin/env python3

import logging
import queue
import signal
import threading

from streva.actor import MeasuredMixin, MonitoredMixin, SupervisorMixin, Actor
from streva.reactor import Reactor, Done


class StopProduction(Exception):
    pass


class Producer(MeasuredMixin, MonitoredMixin, Actor):

    def __init__(self, reactor, name):
        super().__init__(reactor=reactor, name=name)
        self.out = self.make_port("out")
        self.count = 1

    def init(self, _):
        self.add_timeout(self.produce, .00001)

    def produce(self, msg):
        self.out.send(self.count)
        self.count += 1
        self.add_timeout(self.produce, .01)


class Consumer(MeasuredMixin, MonitoredMixin, Actor):

    def __init__(self, reactor, name):
        super().__init__(reactor=reactor, name=name)
        self.add_handler("in", self.on_receive)

    def on_receive(self, msg):
        logging.info("Count is: {}".format(msg))


class Supervisor(SupervisorMixin, Actor):

    def __init__(self, reactor, name):
        super().__init__(reactor=reactor, name=name, timeout_period=.5, probe_period=2)
        self.add_handler("finish", self.finish)
        self.stopped = False

    def error_received(self, err):
        errored_event, error = err
        logging.exception(err)
        self.finish(None)

    def finish(self, _):
        if not self.stopped:
            self.stopped = True
            self.stop_supervised()
            self.stop()

            for actor in self.get_supervised():
                try:
                    actor.print_stats()
                except AttributeError:
                    # Some actors need not have get_stats() because they are
                    # not MeasuredActor
                    pass

            # End all action here
            self._reactor.stop()


def register_stop_signal(supervisor):
    def signal_stop_handler(sig, frame):
        supervisor.send("finish", None, urgent=True)

    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(sig, signal_stop_handler)


if __name__ == "__main__":
    done = queue.Queue()
    reactor = Reactor(done)

    # Define actors
    producer = Producer(reactor, "producer")
    consumer = Consumer(reactor, "consumer")
    supervisor = Supervisor(reactor, "supervisor")

    producer.connect("out", consumer, "in")
    supervisor.supervise(producer)
    supervisor.supervise(consumer)

    # Register keyinterrupt signals to be effective
    register_stop_signal(supervisor)

    # Configure logging
    logging.basicConfig(format="%(levelname)s -- %(message)s", level=logging.INFO)

    # Start the reactor
    reactor.start()
    
    _, sig = done.get()
    try:
        raise sig
    except Done:
        pass

