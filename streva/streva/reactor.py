#!/usr/bin/python3

import heapq
import functools
import logging
import os
import select
import signal
import threading
import time

from streva.observable import Observable
from streva.queue import Queue, Empty


class Done(Exception):
    pass


class Event:

    def __init__(self, function, message, delay=None):
        self.message = message
        self._function = function
        self._on_success = None
        self._on_error = None
        self._processed = False

        self._delay = delay
        if delay is not None:
            self.deadline = time.time() + delay

    def process(self):
        if not self._processed:
            # Make 'error' variable, because if the error notification was in
            # except clause, it would print double exceptions. Something like:
            # Exception happened... during exception another exception happened...
            error = None
            try:
                self._function(self.message)
            except Exception as e:
                error = e

            if error is None:
                if self._on_success:
                    self._on_success(self)
            else:
                if self._on_error:
                    self._on_error((self, error))

        self._processed = True

    def ok(self, cb):
        self._on_success = cb
        return self

    def err(self, cb):
        self._on_error = cb
        return self

    def deactivate(self):
        self._processed = True

    def is_processed(self):
        return bool(self._processed)

    def is_deactivated(self):
        return bool(self._processed)

    def is_timeout(self):
        return bool(self._delay)

    def __lt__(self, other):
        return self.deadline < other.deadline

    def __le__(self, other):
        return self.deadline <= other.deadline


class Reactor(Observable):

    def __init__(self, done):
        super().__init__()

        # Synchronization queue to main thread
        self._done = done

        # Scheduling
        self._queue = Queue()
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
        self.schedule(empty_event)

        self._thread.join()

    def start(self):
        self._thread = threading.Thread(target=self._run)
        self._thread.start()


    # Scheduler methods
    def schedule(self, event, prioritized=False):
        if event.is_timeout():
            heapq.heappush(self._timeouts, event)
        else:
            self._queue.enqueue(event, prioritized)

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
        except Empty:
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


class IOReactor(Reactor):
    """ IOReactor is an extension of Reactor, which can accept and send events
    to outside world through file descriptors. Internal implementation is based
    on 'select.epoll', therefore it only works on machines supporting epoll.
    """

    def __init__(self, done):
        Reactor.__init__(self, done)

        # Epoll object
        self._poll = select.epoll()

        # Register file descriptor for internal events. We need to redirect
        # messages sent to this component's operations to poll through Unix
        # pipe.
        self._inside_events_pipe = os.pipe()
        self._poll.register(self._inside_events_pipe[0], select.POLLIN)

    def schedule(self, event):
        # Schedule the event (put task event into queue)
        Reactor.schedule(self, event)

        if not event.is_timeout():
            # Signal about task event to epoll by sending a random single byte
            # to it
            os.write(self._inside_events_pipe[1], b'X')

    def _process_tasks(self, timeout):
        events = self._poll.poll(timeout)
        if not events:
            # No events -> timeout happened
            return

        for fd, event in events:
            if fd == self._inside_events_pipe[0]:
                # Consume the '\0' byte sent by 'send' method and process the event.
                os.read(fd, 1)
                event = self._queue.get_nowait()
                event.process()
            else:
                self.process_poll_event(fd, event)


    def process_poll_event(self, fd, event):
        raise NotImplementedError("process_poll_event must be overriden")

