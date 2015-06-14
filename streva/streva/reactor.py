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

    def enqueue(self, x, delay=0):
        deadline = time.time() + delay
        super().enqueue((deadline, delay, x), urgent=delay > 0)

    def dequeue(self, block=True, timeout=None):
        while True:
            now = time.time()
            timeout = self._WAIT

            if self.delayed:
                nearest = self.delayed[0]
                timeout = max(0, nearest[0] - now)

            try:
                deadline, delay, x = super().dequeue(block=block, timeout=timeout)
            except queue.Empty:
                if not block:
                    raise
            else:
                if delay > 0:
                    heapq.heappush(self.delayed, (deadline, x))
                else:
                    return x

            delayed = []
            while self.delayed and self.delayed[0][0] <= now:
                delay, x = heapq.heappop(self.delayed)
                delayed.append(x)
            while delayed:
                x = delayed.pop()
                super().enqueue((0, 0, x))


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

    def receive(self, function, **k):
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
        if delay >= 0:
            self._queue.enqueue(function, delay=delay)
        else:
            raise ValueError("'delay' must be a non-negative number!")


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

