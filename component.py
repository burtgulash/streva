#!/usr/bin/python3

import heapq
import functools
import logging
import signal
import sys
import threading
import time
import queue


class Scheduler:

    def __init__(self):
        self._timeouts = []
        self._cancellations = 0

    def on_start(self):
        """ Override """
        pass

    def on_end(self):
        """ Override """
        pass

    def on_after_task(self):
        """ Override """
        pass

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

        for timeout in due_timeouts:
            if timeout.callback is not None:
                timeout.callback()

    def call_later(self, delay, callback, *args, **kwargs):
        return self.call_at(time.time() + delay, callback, *args, **kwargs)

    def call_at(self, deadline, callback, *args, **kwargs):
        timeout = self._Timeout(deadline, functools.partial(callback, *args, **kwargs))
        heapq.heappush(self._timeouts, timeout)
        return timeout

    def remove_timeout(self, timeout):
        timeout.callback = None
        self._cancellations += 1


    class _Timeout:

        __slots__ = ["deadline", "callback"]

        def __init__(self, deadline, callback):
            self.deadline = deadline
            self.callback = callback

        def __lt__(self, other):
            return self.deadline < other.deadline

        def __le__(self, other):
            return self.deadline <= other.deadline


class Messager:

    def __init__(self):
        # Message routing
        self._ports = {}
        self._operations = {}

    def make_port(self, name):
        self._ports[name] = self._Port(name)

    def add_handler(self, name, handler):
        self._operations[name] = handler


    def connect_port(self, port_name, target_component, operation_name):
        """ This method should be only used by connecting mechanism.
        """
        port = self._ports[port_name]
        port._targets.append((target_component, operation_name))

    def send(self, port_name, message):
        port = self._ports[port_name]
        for target_component, operation_name in port._targets:
            target_component.receive(operation_name, message)

    def receive(self, operation_name, message):
        self._tasks.put((operation_name, message))


    class _Port:
        """ Port is a named set of components to all of which an outbound
        message will be sent through this port.
        """

        def __init__(self, name):
            self.name = name
            self._targets = []




class Component(Scheduler, Messager):

    def __init__(self):
        Scheduler.__init__(self)
        Messager.__init__(self)

        self._tasks = queue.Queue()

        self._thread = None
        self._should_run = True

    def stop(self):
        self._should_run = False
        self._thread.join()

    def start(self):
        self._thread = threading.Thread(target=self._run)
        self._thread.start()

    def _run(self):
        self.on_start()

        while self._should_run:
            self.now = time.time()

            time_to_nearest = .1
            if self._timeouts:
                time_to_nearest = max(.1, self._timeouts[0].deadline - self.now)

            try:
                operation_name, message = self._tasks.get(timeout=time_to_nearest)
            except queue.Empty:
                self._process_timeouts()
            else:
                fn = self._operations[operation_name]
                fn(message)

            # print("test")

            self.on_after_task()

        self.on_end()



class Counter(Component):

    def __init__(self, count_from):
        Component.__init__(self)

        self.make_port("count")
        self.count = count_from

    def on_start(self):
        def cb():
            self.send("count", self.count)
            self.count += 1

            self.call_later(1, cb)

        self.call_later(1, cb)


class Printer(Component):

    def __init__(self):
        Component.__init__(self)

        self.add_handler("print", self.on_print)

    def on_print(self, count):
        logging.info("printing " + str(count))
        print("Count is:", count)


if __name__ == "__main__":
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

