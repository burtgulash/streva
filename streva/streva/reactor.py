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


class Reactor:

    NOW = 0.0

    def __init__(self, actors=[]):
        self._queue = UrgentQueue()
        self.__thread = None

        for actor in actors:
            self.set_actor(actor)

    def set_actor(self, actor):
        actor.set_reactor(self)

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

    def receive(self, function, when):
        event = Event(function)
        self._schedule(event)

    def _schedule(self, event, skip=False):
        self._queue.enqueue(event, urgent=skip)

    def _react(self):
        event = self._queue.dequeue()
        event.function()


class LoopReactor(Reactor):

    def __init__(self, actors=[]):
        super().__init__(actors=actors)
        self.__running = False

    def stop(self):
        self.__running = False

        # Flush the queue with empty event
        self.receive(lambda: None, Reactor.NOW)

    def _react(self):
        self.__running = True
        while self.__running:
            self._iteration()

    def _iteration(self):
        event = self._queue.get()
        event.function()


class TimedReactor(LoopReactor):

    def __init__(self, actors=[]):
        super().__init__(actors=actors)

        # To avoid busy waiting, wait this number of seconds if there is no
        # event to process in an iteration.
        self._WAIT_ON_EMPTY = .5
        self._delayed = []

    def receive(self, function, when):
        if when > 0:
            event = DelayedEvent(function, when)
            self._schedule(event, skip=True)
        elif when == 0:
            super().receive(function, when)
        else:
            raise ValueError("'when' must be a non-negative number!")

    def _iteration(self):
        now = time.time()

        timeout = self._WAIT_ON_EMPTY
        # Find timeout - time to nearest scheduled timeout or default
        # to WAIT_ON_EMPTY queue period
        if self._delayed:
            timeout = max(0, self._delayed[0].deadline - now)

        try:
            event = self._queue.dequeue(timeout=timeout)
        except queue.Empty:
            # Timeout obtained means that a delayed event came before
            # an event from the queue
            pass
        else:
            if isinstance(event, DelayedEvent):
                heapq.heappush(self._delayed, event)
            else:
                event.function()

        # Delayed events are processed after normal events, so that urgent
        # messages are processed first
        while self._delayed and self._delayed[0].deadline <= now:
            delayed = heapq.heappop(self._delayed)
            delayed.function()


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

