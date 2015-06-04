#!/usr/bin/env python3

import logging

from streva.reactor import *
from streva.actor import *
from streva.supervisor import *

#
# Basic test so that this module can be tested immediately
class Counter(MonitoredMixin, Actor):
    """ Sample implementation of Actor which generates sequence of numbers
    in periodic intervals and sends them out for printing.
    """

    def __init__(self, reactor, count_from, name):
        super().__init__(reactor=reactor, name=name)
        self.out_port = self.make_port("count")
        self.count = count_from

    def init(self, message):
        def cb(_):
            self.out_port.send(self.count)
            self.count += 1

            # Loop pattern
            self.add_timeout(cb, .1)
        self.add_timeout(cb, .1)


class Printer(MeasuredMixin, Actor):
    """ Sample implementation of Actor which simply prints numbers received
    from Counter.
    """

    def __init__(self, reactor, name):
        super().__init__(reactor=reactor, name=name)
        self.add_handler("print", self.on_print)

    def on_print(self, count):
        logging.info("printing " + str(count))
        print("Count is:", count)


def test():
    # Define engines
    reactor = Reactor()
    io_reactor = IOReactor()

    # Define logical components
    counter = Counter(reactor, 1, "counter")
    printer = Printer(io_reactor, "printer")

    # Wire components together.
    # eg. subscribe 'printer.print' to 'counter.count'
    counter.connect("count", printer, "print")


    # Set up logging
    logging.basicConfig(format="%(levelname)s -- %(message)s",
                        level=logging.INFO)

    # Register all components within supervisor and start them
    supervisor = Emperor()
    supervisor.add(reactor)
    supervisor.add(io_reactor)

    supervisor.start_all()


if __name__ == "__main__":
    test()

