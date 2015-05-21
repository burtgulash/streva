#!/usr/bin/python3

import heapq
import functools
import logging
import os
import signal
import select
import sys
import threading
import time
import queue




class Stats:

    def __init__(self):
        self.operation_stats = {}
        self.queue_size = 0

    def register_operation_stats(self, operation_name):
        self.operation_stats[operation_name] = Stats.OperationStats()

    def update_running_stats(self, operation_name, running_time, number_of_runs):
        op_stats = self.operation_stats[operation_name]
        op_stats.runs += number_of_runs
        op_stats.total_time += running_time

    # TODO dump stats into serialized form

    class OperationStats:

        def __init__(self):
            self.runs = 0
            self.total_time = 0



class Component:
    """ Component class is an implementation of scheduled event loop.

    A component can be sent messages to, which are then in turn handled by
    component's handlers. Sleeping on the thread is implemented by timeouts,
    which are callbacks delayed on the component's scheduler calendar.

    Components can route outgoing messages through Ports. Port is a publisher
    mechanism, which sends messages to its subscribers.
    """

    def __init__(self):

        # Scheduling
        self._tasks = queue.Queue()
        # To avoid busy waiting, wait this number of seconds if there is no
        # task or timeout to process in an iteration
        self._WAIT_ON_EMPTY = .5
        self._timeouts = []
        self._cancellations = 0

        # Message routing
        self._ports = {}
        self._operations = {}

        # Monitoring
        self.stats = Stats()
        self.stats.register_operation_stats("timeouts")

        # Running
        self._thread = None
        self._is_dead = False
        self._should_run = True

    def make_port(self, name):
        port = self._Port(name)
        self._ports[name] = port
        return port

    def add_handler(self, operation_name, handler):
        self._operations[operation_name] = handler
        self.stats.register_operation_stats(operation_name)

    def connect(self, port_name, to_component, to_operation_name):
        port = self._ports[port_name]
        port._targets.append((to_component, to_operation_name))

    def subscribe(self, operation_name, source_component, source_port_name):
        source_component.connect(source_port_name, self, operation_name)

    def send(self, operation_name, message):
        """ Send message to this component's operation.
        """
        self._tasks.put((operation_name, message))

    def stop(self):
        self._should_run = False
        self._thread.join()

    def start(self):
        self._thread = threading.Thread(target=self._run)
        self._thread.start()

    def is_dead(self):
        return self._is_dead

    def on_start(self):
        """ Override """
        pass

    def on_end(self):
        """ Override """
        pass

    def on_after_task(self):
        """ Override """
        pass

    def _process_operation(self, operation_name, message):
        fn = self._operations[operation_name]
        fn(message)

        running_time = time.time() - self.now
        self.stats.update_running_stats(operation_name, running_time, 1)

    def _process_timeouts(self):
        due_timeouts = []
        while self._timeouts:
            if self._timeouts[0].callback is None:
                heapq.heappop(self._timeouts)
                self._cancellations -= 1
            elif self._timeouts[0].deadline <= self.now:
                due_timeouts.append(heapq.heappop(self._timeouts))
            else:
                break
        if self._cancellations > 512 and \
           self._cancellations > (len(self._timeouts) >> 1):
                self._cancellations = 0
                self._timeouts = [x for x in self._timeouts
                                  if x.callback is not None]
                heapq.heapify(self._timeouts)

        no_timeouts = len(due_timeouts)
        for timeout in due_timeouts:
            if timeout.callback is not None:
                timeout.callback()

        running_time = time.time() - self.now
        self.stats.update_running_stats("timeouts", running_time, no_timeouts)


    def call_later(self, delay, callback, *args, **kwargs):
        return self.call_at(time.time() + delay, callback, *args, **kwargs)

    def call_at(self, deadline, callback, *args, **kwargs):
        timeout = self._Timeout(deadline, functools.partial(callback, *args, **kwargs))
        heapq.heappush(self._timeouts, timeout)
        return timeout

    def remove_timeout(self, timeout):
        timeout.callback = None
        self._cancellations += 1

    def _run(self):
        self.on_start()

        try:
            while self._should_run:
                self.now = time.time()

                time_to_nearest = self._WAIT_ON_EMPTY
                if self._timeouts:
                    time_to_nearest = max(0, self._timeouts[0].deadline - self.now)

                # This is where all the action happens.
                has_timeouted = self._process_events(time_to_nearest)

                if self._timeouts:
                    self._process_timeouts()

                self.on_after_task()
                self.stats.queue_size = self._tasks.qsize()
        except:
# http://stackoverflow.com/questions/5191830/python-exception-logging#comment5837573_5191885
            logging.exception("Component failed on exception!")

        self._is_dead = True
        self.on_end()

    def _process_events(self, timeout):
        """ Process events from component's queue.
        Return True if timeouted, False otherwise.
        """

        try:
            operation_name, message = self._tasks.get(timeout=timeout)
        except queue.Empty:
            # Timeout obtained means that a timeout event came before an event
            # from the queue
            return True
        else:
            self._process_operation(operation_name, message)
            return False


    class _Timeout:

        __slots__ = ["deadline", "callback"]

        def __init__(self, deadline, callback):
            self.deadline = deadline
            self.callback = callback

        def __lt__(self, other):
            return self.deadline < other.deadline

        def __le__(self, other):
            return self.deadline <= other.deadline


    class _Port:
        """ Port is a named set of components to all of which an outbound
        message will be sent through this port. Port implements pubsub routing.
        """

        def __init__(self, name):
            self.name = name
            self._targets = []

        def send(self, message):
            """ Send message to all connected components through this pubsub port.
            """
            for target_component, operation_name in self._targets:
                target_component.send(operation_name, message)


class IOComponent(Component):
    """ Component which can accept and send events to outside world through
    file descriptors.  Internal implementation is based on 'select.epoll',
    therefore it only works on machines supporting epoll.
    """

    def __init__(self):
        Component.__init__(self)

        # Epoll object
        self._poll = select.epoll()

        # Register file descriptor for operations. We need to redirect messages
        # sent to this component's operations to poll through Unix pipe.
        self._operations_pipe = os.pipe()
        self._poll.register(self._operations_pipe[0], select.POLLIN)

    def send(self, operation_name, message):
        # Put task message into queue
        Component.send(self, operation_name, message)
        # Signal about this event to epoll by sending a single byte to it
        os.write(self._operations_pipe[1], b'\0')

    def _process_events(self, timeout):
        events = self._poll.poll(timeout)
        if not events:
            # No events -> timeout happened
            return True

        for fd, event in events:
            if fd == self._operations_pipe[0]:
                # Consume the '\0' byte sent by 'send' method and process the operation
                os.read(fd, 1)
                operation_name, message = self._tasks.get_nowait()
                self._process_operation(operation_name, message)
            else:
                self.process_poll_event(fd, event)

        return False

    def process_poll_event(self, fd, event):
        raise NotImplementedError("process_poll_event must be overriden")



class Supervisor:

    def __init__(self):
        self._components = []

        # Register signal handler for stop signals
        def signal_stop_handler(sig, frame):
            logging.info("STOP signal received.")
            self.stop_all()

        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
            signal.signal(sig, signal_stop_handler)

    def add(self, component):
        self._components.append(component)

    def start_all(self):
        for component in self._components:
            component.start()

    def stop_all(self):
        for component in self._components:
            component.stop()




# T
# TE
# TES
# TEST
#
# Basic test so that this module can be tested immediately
class Counter(Component):
    """ Sample implementation of Component which generates sequence of numbers
    in periodic intervals and sends them out for printing.
    """

    def __init__(self, count_from):
        super().__init__()

        self.out_port = self.make_port("count")
        self.count = count_from

    def on_start(self):
        def cb():
            self.out_port.send(self.count)
            self.count += 1

            self.call_later(1, cb)

        self.call_later(1, cb)


class Printer(IOComponent):
    """ Sample implementation of Component which simply prints numbers received
    from Counter.
    """

    def __init__(self):
        super().__init__()

        self.add_handler("print", self.on_print)
        self.out_port = self.make_port("out")

    def on_print(self, count):
        logging.info("printing " + str(count))
        print("Count is:", count)
        self.out_port.send(count)


class SquaredPrinter(Component):
    """ Square a number and print it. """

    def __init__(self):
        super().__init__()

        self.add_handler("print", self.on_print)

    def on_print(self, count):
        count = count * count
        logging.info("square printing " + str(count))
        print("Count is:", count)


def test():
    counter = Counter(1)
    printer = Printer()
    sq_printer = SquaredPrinter()

    # Wire components together.
    # eg. subscribe 'printer.print' to 'counter.count'
    counter.connect("count", printer, "print")
    printer.connect("out", sq_printer, "print")

    # Set up logging
    logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s",
                        datefmt="%I:%M:%S",
                        level=logging.INFO)

    # Register all components within supervisor and start them
    supervisor = Supervisor()
    supervisor.add(counter)
    supervisor.add(printer)
    supervisor.add(sq_printer)

    supervisor.start_all()


if __name__ == "__main__":
    test()

