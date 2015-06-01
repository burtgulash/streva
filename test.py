#!/usr/bin/env python3

import logging

from streva.reactor import *
from streva.component import *
from streva.supervisor import *

#
# Basic test so that this module can be tested immediately
class Counter(Component):
    """ Sample implementation of Component which generates sequence of numbers
    in periodic intervals and sends them out for printing.
    """

    def __init__(self, count_from, name=None, reactor=None):
        super().__init__(reactor, name=name)

        self.out_port = self.make_port("count")

        self.count = count_from

    def on_start(self, message):
        def cb():
            self.out_port.send(self.count)
            self.count += 1

            self.call(cb, delay=.1)

        self.call(cb, delay=.1)


class Printer(Component):
    """ Sample implementation of Component which simply prints numbers received
    from Counter.
    """

    def __init__(self, name=None, reactor=None):
        super().__init__(reactor, name=name)

        self.add_handler("print", self.on_print)
        self.out_port = self.make_port("out")

    def on_print(self, count):
        logging.info("printing " + str(count))
        print("Count is:", count)
        self.out_port.send(count)


def test():
    # Define engines
    reactor = Reactor()
    io_reactor = IOReactor()

    # Define logical components
    counter = Counter(1, name="counter", reactor=reactor)
    printer = Printer(name="printer", reactor=io_reactor)

    # Wire components together.
    # eg. subscribe 'printer.print' to 'counter.count'
    counter.connect("count", printer, "print")

    # Set up logging
    logging.basicConfig(format="%(levelname)s -- %(message)s",
                        level=logging.INFO)

    # Register all components within supervisor and start them
    supervisor = Supervisor()
    supervisor.add(reactor)
    supervisor.add(io_reactor)

    supervisor.start_all()


if __name__ == "__main__":
    test()

