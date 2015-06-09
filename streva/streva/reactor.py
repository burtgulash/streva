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

from streva.observable import Observable


class Done(Exception):
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


class UrgentQueue(queue.Queue):

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


class Reactor(Observable):

    def __init__(self, done):
        super().__init__()

        # Synchronization queue to main thread
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
        self.notify("start", None)

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
        finally:
            self.notify("end", None)


class SocketReactor(Reactor):

    def __init__(self, done):
        Reactor.__init__(self, done)

        # Epoll object
        self._poll = select.epoll()

        # Register file descriptor for internal events. We need to redirect
        # messages sent to this component's operations to poll through Unix
        # pipe.
        self._inside_events_pipe = os.pipe()
        self._poll.register(self._inside_events_pipe[0], select.EPOLLIN)

        self._fd_handlers = {}

    def add_socket(self, fd, handler):
        self._fd_handlers[fd] = handler
        self._poll.register(fd, select.EPOLLIN)

    def del_socket(self, fd):
        del self._fd_handlers[fd]
        self._poll.unregister(fd)

    def schedule(self, event):
        # Schedule the event (put task event into queue)
        Reactor.schedule(self, event)

        if not event.is_timeout():
            # Signal about task event to epoll by sending a random single byte
            # to it
            os.write(self._inside_events_pipe[1], b'X')

    def _process_poll_event(fd, event):
        handler = self._fd_handlers[fd]

    def _process_tasks(self, timeout):
        events = self._poll.poll(timeout)
        if not events:
            # No events -> timeout happened
            return

        for fd, event in events:
            if fd == self._inside_events_pipe[0]:
                # Consume the '\0' byte sent by 'send' method and process the event.
                os.read(fd, 1)
                ev = self._queue.get_nowait()
                ev.process()
            else:
                self._process_poll_event(fd, event)


