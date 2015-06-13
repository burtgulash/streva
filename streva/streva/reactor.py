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


URGENT = -1
NORMAL = 0


class Done(Exception):
    """ Done is an exception signaled when a loop finishes
    successfully.
    """
    pass


class Event:

    __slots__ = "function"

    def __init__(self, function):
        self.function = function

    def __repr__(self):
        return "Event({})".format(self.function)


class DelayedEvent(Event):

    __slots__ = "deadline", "delay"

    def __init__(self, function, delay):
        super().__init__(function)
        self.deadline = time.time()
        self.delay = delay or 0
        if delay and delay >= 0:
            self.deadline += delay

    def __lt__(self, other):
        return self.deadline < other.deadline

    def __le__(self, other):
        return self.deadline <= other.deadline

    def __repr__(self):
        return "DelayedEvent({}, after={})".format(self.function, self.delay)


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


class Loop(Observable):

    def __init__(self):
        super().__init__()

        # Synchronization queue to emperor thread
        self.done_queue = None

        # Scheduling
        self._queue = UrgentQueue()

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
        self.schedule(lambda: None, schedule=NORMAL)

    def schedule(self, function, schedule):
        event = Event(function)
        self._queue.enqueue(event, urgent=(schedule == URGENT))

    def _run(self):
        if self.done_queue:
            try:
                self._loop()
            except Exception as err:
                self.done_queue.put((self, err))
            else:
                self.done_queue.put((self, Done()))
        else:
            self._loop()

    def _loop(self):
        while self._should_run:
            self._iteration()

    def _iteration(self):
        event = self._queue.dequeue(block=True)
        event.function()



class TimedLoop(Loop):

    def __init__(self):
        super().__init__()

        # To avoid busy waiting, wait this number of seconds if there is no
        # task or timeout to process in an iteration.
        self._WAIT_ON_EMPTY = .5
        self._timeouts = []

    def schedule(self, function, schedule):
        if schedule > 0:
            event = DelayedEvent(function, schedule)
            heapq.heappush(self._timeouts, event)
        elif schedule <= 0:
            super().schedule(function, schedule)
        else:
            raise ValueError("'schedule' must be a number!")

    def _iteration(self):
        now = time.time()

        timeout = self._WAIT_ON_EMPTY
        # Find timeout - time to nearest scheduled timeout or default
        # to WAIT_ON_EMPTY queue period
        if self._timeouts:
            timeout = max(0, self._timeouts[0].deadline - now)

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
        while self._timeouts and self._timeouts[0].deadline <= now:
            timeout = heapq.heappop(self._timeouts)
            timeout.function()



class Emperor:

    def __init__(self):
        self.done_queue = queue.Queue()
        self._loops = set()

    def add_loop(self, loop):
        self._loops.add(loop)
        loop.synchronize(self.done_queue)

    def start(self):
        for loop in self._loops:
            loop.start()

    def stop(self):
        for loop in self._loops:
            loop.stop()

    def join(self):
        for x in self._loops:
            loop, sig = self.done_queue.get()
            try:
                raise sig
            except Done:
                pass
            except:
                # Explicitly raise
                raise

