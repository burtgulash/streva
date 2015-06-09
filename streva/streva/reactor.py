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


class Loop:

    def __init__(self, name, done):
        # Synchronization queue to main thread
        self.name = name
        self._done = done

        # Scheduling
        self._queue = UrgentQueue()
        # To avoid busy waiting, wait this number of seconds if there is no
        # task or timeout to process in an iteration.
        self._WAIT_ON_EMPTY = .5
        self._timeouts = []

        # Running
        self._thread = None
        self._should_run = True

    # Lifecycle methods
    def stop(self):
        self._should_run = False

        # Flush the queue with empty message if it was waiting for a timeout
        empty_event = Event(lambda _: None, None)
        self.schedule(empty_event, NORMAL)

    def start(self):
        self._thread = threading.Thread(target=self._run)
        self._thread.start()


    # Scheduler methods
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

    def _process_timeouts(self):
        while self._timeouts:
            if self._timeouts[0].is_deactivated():
                heapq.heappop(self._timeouts)
            elif self._timeouts[0].deadline <= self.now:
                timeout = heapq.heappop(self._timeouts)
                timeout.process()
            else:
                break

    def _process_tasks(self, timeout):
        try:
            event = self._queue.dequeue(timeout=timeout)
        except queue.Empty:
            # Timeout obtained means that a timeout event came before an event
            # from the queue
            pass
        else:
            event.process()

    def _run(self):
        try:
            while self._should_run:
                self.now = time.time()

                timeout = self._WAIT_ON_EMPTY
                # Find timeout - time to nearest scheduled timeout or default
                # to WAIT_ON_EMPTY queue period
                if self._timeouts:
                    timeout = max(0, self._timeouts[0].deadline - self.now)

                # This is where all the action happens.
                self._process_tasks(timeout)

                if self._timeouts:
                    self._process_timeouts()
        except Exception as err:
            self._done.put((self, err))
        else:
            self._done.put((self, Done()))


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

    def __init__(self, num_blocking=1):
        super().__init__()

        self.num_blocking = num_blocking
        self._done = queue.Queue()
        self._block_loops = set()

        self._main_loop = Loop("main", self._done)
        for x in range(num_blocking):
            loop = Loop("blocking_" + str(x + 1), self._done)
            self._block_loops.add(loop)

    def get_loops(self):
        return self._block_loops | set([self._main_loop])

    def start(self):
        self.notify("start", None)
        for loop in self.get_loops():
            loop.start()

    def stop(self):
        for loop in self.get_loops():
            loop.stop()

    def join(self):
        for x in self.get_loops():
            loop, sig = self._done.get()
            try:
                raise sig
            except Done:
                pass
            except:
                # Explicitly raise
                raise
        self.notify("end", None)

    def schedule(self, event, schedule, is_blocking=False):
        if is_blocking:
            import random
            loops = self.get_loops()
            chosen = loops[random.randint(0, len(loops))]
        else:
            chosen = self._main_loop

        chosen.schedule(event, schedule)

