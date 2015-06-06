#!/usr/bin/env python3

from streva.actor import MeasuredMixin, SupervisorMixin, Actor
from streva.reactor import Reactor
import logging
import signal
import threading


# Synchronize by locking (or sending a message metaphorically) between reactor
# threads and main thread
wait = threading.Lock()


class StopProduction(Exception):
    pass


class Producer(MeasuredMixin, Actor):

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


class Consumer(MeasuredMixin, Actor):

    def __init__(self, reactor, name):
        super().__init__(reactor=reactor, name=name)
        self.add_handler("in", self.on_receive)

    def on_receive(self, msg):
        print("Count is:", msg)


class Supervisor(SupervisorMixin, Actor):

    def __init__(self, reactor, name):
        super().__init__(reactor=reactor, name=name, timeout_period=.5, probe_period=2)
        self.stopped = False

    def error_received(self, err):
        errored_event, error = err
        logging.exception(err)
        self.finish(None)

    def finish(self, _):
        if not self.stopped:
            self.stop_supervised()
            self.stop()

            for actor in self.get_supervised():
                try:
                    actor.print_stats()
                except AttributeError:
                    pass

            self.stopped = True
            wait.release()


def register_stop_signal(supervisor):
    def signal_stop_handler(sig, frame):
        logging.info("STOP signal received.")
        supervisor.finish(None)

    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(sig, signal_stop_handler)


if __name__ == "__main__":
    reactor = Reactor()

    # Define actors
    producer = Producer(reactor, "producer")
    consumer = Consumer(reactor, "consumer")
    supervisor = Supervisor(reactor, "supervisor")

    producer.connect("out", consumer, "in")
    supervisor.supervise(producer)
    supervisor.supervise(consumer)

    register_stop_signal(supervisor)

    wait.acquire()
    reactor.start()

    wait.acquire()
    reactor.stop()

