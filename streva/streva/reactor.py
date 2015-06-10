#!/usr/bin/python3

import collections
import heapq
import functools
import logging
import os
import queue
import select
import signal
import threading
import time


class Done(Exception):
    """ Done is an exception signaled when a loop finishes
    successfully.
    """
    pass


URGENT = -1
NORMAL = 0

class Cancellable:

    nop = lambda: None

    def __init__(self, f):
        self.f = f
        self.cleanup = self.nop
        self.cancelled = False
        self.finished = False

    def __call__(self):
        print("__CALL__", self.f, self.cancelled)
        if not self.cancelled:
            self.f()
        self.cleanup()
        self.finished = True

    def add_cleanup_f(self, cleanup_f):
        self.cleanup = cleanup_f

    def cancel(self):
        self.finished = True

    def is_finished(self):
        return self.finished
    
    def is_canceled(self):
        return self.cancelled


class Event:

    def __init__(self, function, delay=None):
        self.function = function
        self.delayed = False
        self.deadline = time.time()

        if delay:
            self.delayed = True
            self.deadline += delay

    def is_delayed(self):
        return self.delayed

    def __lt__(self, other):
        return self.deadline < other.deadline

    def __le__(self, other):
        return self.deadline <= other.deadline


class UrgentQueue(queue.Queue):
    """ Implementation of blocking queue which can queue urgent items to the
    beginning of the queue instead of at the end.
    """

    def __init__(self):
        super().__init__()
        self.queue = collections.deque()

    def enqueue(self, x, urgent=False):
        item = x, urgent
        self.put(item)

    def dequeue(self, block=True, timeout=None):
        return self.get(block, timeout)

    def _put(self, item):
        x, urgent = item
        if urgent:
            self.queue.appendleft(x)
        else:
            self.queue.append(x)


class Observable:

    def __init__(self):
        self._observers = {}

    def notify(self, event_name):
        if event_name in self._observers:
            for handler in self._observers[event_name]:
                handler()

    def add_observer(self, event_name, handler):
        if event_name not in self._observers:
            self._observers[event_name] = []
        self._observers[event_name].append(handler)

    def del_observer(self, event_name, handler):
        if event_name in self._observers:
            without_observer = []
            for h in self._observers[event_name]:
                if h != handler:
                    without_observer.append(h)
            self._observers[event_name] = without_observer


class Reactor(Observable):

    def __init__(self):
        super().__init__()

        # Synchronization queue to emperor thread
        self.done_queue = None

        # Scheduling
        self._queue = UrgentQueue()
        # To avoid busy waiting, wait this number of seconds if there is no
        # task or timeout to process in an iteration.
        self._WAIT_ON_EMPTY = .5
        self._timeouts = []

        # Running
        self._thread = None
        self._should_run = True

    def synchronize(self, done):
        self.done_queue = done

    def start(self):
        self._thread = threading.Thread(target=self._run)
        self._thread.start()

    def stop(self):
        self._should_run = False

        # Flush the queue with empty message if it was waiting for a timeout
        empty_event = Event(lambda: None)
        self.schedule(empty_event, NORMAL)

    def schedule(self, function, schedule):
        if schedule > 0:
            event = Event(function, delay=schedule)
            heapq.heappush(self._timeouts, event)
        else:
            event = Event(function)
            if schedule < 0:
                self._queue.enqueue(event, urgent=True)
            elif schedule == 0:
                self._queue.enqueue(event)
            else:
                raise ValueError("Schedule must be a number!.")

    def _run(self):
        self.notify("start")

        if self.done_queue:
            try:
                self._loop()
            except Exception as err:
                self.done_queue.put((self, err))
            else:
                self.done_queue.put((self, Done()))
        else:
            self._loop()

        self.notify("end")

    def _loop(self):
        while self._should_run:
            self.now = time.time()

            timeout = self._WAIT_ON_EMPTY
            # Find timeout - time to nearest scheduled timeout or default
            # to WAIT_ON_EMPTY queue period
            if self._timeouts:
                timeout = max(0, self._timeouts[0].deadline - self.now)

            try:
                event = self._queue.dequeue(timeout=timeout)
            except queue.Empty:
                # Timeout obtained means that a timeout event came before
                # an event from the queue
                pass
            else:
                event.function()

            # Timeouts are processed after normal events, so that urgent
            # messages are processed first
            while self._timeouts:
                if self._timeouts[0].function.is_canceled():
                    heapq.heappop(self._timeouts)
                elif self._timeouts[0].deadline <= self.now:
                    timeout = heapq.heappop(self._timeouts)
                    timeout.function()
                else:
                    break


class Emperor:

    def __init__(self):
        self.done_queue = queue.Queue()
        self._reactors = set()

    def add_reactor(self, reactor):
        self._reactors.add(reactor)
        reactor.synchronize(self.done_queue)

    def start(self):
        for reactor in self._reactors:
            reactor.start()

    def stop(self):
        for reactor in self._reactors:
            reactor.stop()

    def join(self):
        for x in self._reactors:
            reactor, sig = self.done_queue.get()
            try:
                raise sig
            except Done:
                pass
            except:
                # Explicitly raise
                raise

