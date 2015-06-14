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


class TimedQueue(UrgentQueue):

    def __init__(self):
        super().__init__()
        self.delayed = []
        self._WAIT = .1

    def enqueue(self, x, delayed=0):
        super().enqueue((x, delayed), urgent=delayed > 0)

    def dequeue(self, block=True, timeout=None):
        while True:
            now = time.time()
            timeout = self._WAIT

            if self.delayed:
                nearest = self._delayed[0]
                timeout = max(0, nearest[1] - now)

            try:
                x, delay = super().dequeue(block=block, timeout=timeout)
            except queue.Empty:
                if not block:
                    raise
            else:
                if delay > 0:
                    heapq.heappush(self.delayed, (delay, x))
                else:
                    return x

            delayed = []
            while self.delayed and self.delayed[0][1] <= now:
                delay, x = heapq.heappop(self.delayed)
                delayed.append(x)
            while delayed:
                x = delayed.pop()
                super().enqueue((x, 0))


class Reactor:

    NOW = 0.0

    def __init__(self, processes=[]):
        self._queue = self._make_queue()
        self.__thread = None

        for process in processes:
            self.set_process(process)

    def _make_queue(self):
        return UrgentQueue()

    def set_process(self, process):
        process.set_reactor(self)

    def start(self):
        self._react()

    def stop(self):
        # Can't stop this!
        pass

    def spawn(self, wait):
        self.__thread = threading.Thread(target=self._synchronized, args=[wait])
        self.__thread.start()

    def join(self):
        self.__thread.join()

    def _synchronized(self, wait):
        result = None

        try:
            self.start()
        except Exception as err:
            result = err
        else:
            result = Done

        wait.put((self, result))

    def receive(self, function):
        self._queue.enqueue(function)

    def _react(self):
        function = self._queue.dequeue()
        function()


class LoopReactor(Reactor):

    def __init__(self, processes=[]):
        super().__init__(processes=processes)
        self.__running = False

    def stop(self):
        self.__running = False

        # Flush the queue with empty event
        self.receive(lambda: None)

    def _react(self):
        self.__running = True
        while self.__running:
            self._iteration()

    def _iteration(self):
        function = self._queue.dequeue()
        function()


class TimedReactor(LoopReactor):

    def _make_queue(self):
        return TimedQueue()

    def receive(self, function, delay=0):
        if delay > 0:
            self._schedule(function, delay)
        elif delay == 0:
            self._schedule(function)
        else:
            raise ValueError("'delay' must be a non-negative number!")

    def receive(self, function, delay=0):
        self._queue.enqueue(function, delay=delay)


class Emperor:

    def __init__(self, children=[]):
        self.__synchro = queue.Queue()
        self.__reactors = set(children)

    def add_reactor(self, reactor):
        self.__reactors.add(reactor)

    def start(self):
        for reactor in self.__reactors:
            reactor.spawn(self.__synchro)

    def stop(self):
        for reactor in self.__reactors:
            reactor.stop()

    def join(self):
        for x in range(len(self.__reactors)):
            reactor, result = self.__synchro.get()
            try:
                raise result
            except Done:
                pass
            except:
                self.stop()
                # Explicitly re-raise catched exception
                raise

