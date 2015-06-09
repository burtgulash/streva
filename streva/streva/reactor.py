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

class Event:

    def __init__(self, function, message, delay=None):
        self.message = message
        self._function = function
        self._processed = False

        self._delay = delay
        self.deadline = None

    def process(self):
        if not self._processed:
            self._function(self.message)
        self._processed = True

    def deactivate(self):
        self._processed = True

    def add_delay(self, delay):
        self._delay = delay
        if delay is not None:
            self.deadline = time.time() + delay


    def is_processed(self):
        return bool(self._processed)

    def is_deactivated(self):
        return bool(self._processed)

    def is_timeout(self):
        return bool(self._delay)

    def __repr__(self):
        msg = str(self.message)[:30]
        if self.is_timeout():
            return "Delayed Event({}, {}, {})".format(self._function, msg, self._delay)
        return "Event({}, {})".format(self._function, msg)

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

    def notify(self, event_name, message):
        if event_name in self._observers:
            for handler in self._observers[event_name]:
                handler(message)

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
        empty_event = Event(lambda _: None, None)
        self.schedule(empty_event, NORMAL)

    def schedule(self, event, schedule):
        if schedule > 0:
            event.add_delay(schedule)
            heapq.heappush(self._timeouts, event)
        elif schedule < 0:
            self._queue.enqueue(event, urgent=True)
        elif schedule == 0:
            self._queue.enqueue(event)
        else:
            raise ValueError("Schedule must be a number!.")

    def remove_event(self, event):
        event.deactivate()

    def _run(self):
        self.notify("start", None)

        if self.done_queue:
            try:
                self._loop()
            except Exception as err:
                self.done_queue.put((self, err))
            else:
                self.done_queue.put((self, Done()))
        else:
            self._loop()

        self.notify("end", None)

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
                event.process()

            # Timeouts are processed after normal events, so that urgent
            # messages are processed first
            while self._timeouts:
                if self._timeouts[0].is_deactivated():
                    heapq.heappop(self._timeouts)
                elif self._timeouts[0].deadline <= self.now:
                    timeout = heapq.heappop(self._timeouts)
                    timeout.process()
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

