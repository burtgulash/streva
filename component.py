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

    def connect_port(self, port_name, target_component, operation_name):
        """ Wires channels between components.

        This method should only be used by connecting mechanism.
        """
        port = self._ports[port_name]
        port._targets.append((target_component, operation_name))

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
        if self._timeouts:
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
                self._loop_iteration()
        except:
# http://stackoverflow.com/questions/5191830/python-exception-logging#comment5837573_5191885
            logging.exception("Component failed on exception!")

        self._is_dead = True
        self.on_end()

    def _loop_iteration(self):
        self.now = time.time()

        time_to_nearest = self._WAIT_ON_EMPTY
        if self._timeouts:
            time_to_nearest = max(0, self._timeouts[0].deadline - self.now)

        try:
            operation_name, message = self._tasks.get(timeout=time_to_nearest)
        except queue.Empty:
            self._process_timeouts()
        else:
            self._process_operation(operation_name, message)

        self.on_after_task()
        self.stats.queue_size = self._tasks.qsize()


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
        # Signal about this event to epoll by sending a byte to it
        os.write(self._operations_pipe[1], b'\0')

    def process_poll_event(self, fd, event):
        raise NotImplementedError("process_poll_event must be overriden")

    def _loop_iteration(self):
        self.now = time.time()

        time_to_nearest = self._WAIT_ON_EMPTY
        if self._timeouts:
            time_to_nearest = max(0, self._timeouts[0].deadline - self.now)

        events = self._poll.poll(time_to_nearest)
        for fd, event in events:
            if fd == self._operations_pipe[0]:
                # Consume the '\0' byte and process the operation
                os.read(fd, 1)
                operation_name, message = self._tasks.get_nowait()
                self._process_operation(operation_name, message)
            else:
                self.process_poll_event(fd, event)

        self._process_timeouts()

        self.on_after_task()
        self.stats.queue_size = self._tasks.qsize()


        




# T
# TE
# TES
# TEST
#
# Basic test so that this module can be tested immediately
class Counter(IOComponent):
    """ Sample implementation of Component which generates sequence of numbers
    in periodic intervals and sends them out for printing.
    """

    def __init__(self, count_from):
        IOComponent.__init__(self)

        self.out_port = self.make_port("count")
        self.count = count_from

    def on_start(self):
        def cb():
            self.out_port.send(self.count)
            self.count += 1

            self.call_later(1, cb)

        self.call_later(1, cb)


class Printer(Component):
    """ Sample implementation of Component which simply prints numbers received
    from Counter.
    """

    def __init__(self):
        Component.__init__(self)

        self.add_handler("print", self.on_print)

    def on_print(self, count):
        logging.info("printing " + str(count))
        print("Count is:", count)
        # raise Exception("kkt")

def test():
    counter = Counter(1)
    printer = Printer()

    counter.connect_port("count", printer, "print")


    logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s",
                        datefmt="%I:%M:%S",
                        level=logging.INFO)

    def signal_stop_handler(sig, frame):
        logging.info("STOP signal received.")
        counter.stop()
        printer.stop()

    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(sig, signal_stop_handler)


    # START!
    counter.start()
    printer.start()


if __name__ == "__main__":
    test()
