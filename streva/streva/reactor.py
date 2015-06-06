#!/usr/bin/python3

import heapq
import functools
import logging
import os
import select
import signal
import threading
import time
import queue

from .observable import Observable



class Event:

    __slots__ = ["message",                                 # event payload - message
                 "_function", "_on_success", "_on_error",   # callbacks
                 "deadline", "_delay",                      # timeout time attributes
                 "_processed"]                              # processed flag

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

    def is_timeout(self):
        return bool(self._delay)

    def __lt__(self, other):
        return self.deadline < other.deadline

    def __le__(self, other):
        return self.deadline <= other.deadline


class Emperor:

    def __init__(self):
        self._reactors = []

        # Register signal handler for stop signals
        def signal_stop_handler(sig, frame):
            logging.info("STOP signal received.")
            self.stop_all()

        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
            signal.signal(sig, signal_stop_handler)

    def add(self, reactor):
        self._reactors.append(reactor)

    def start_all(self):
        for reactor in self._reactors:
            reactor.start()

    def stop_all(self):
        for reactor in self._reactors:
            reactor.stop()


class Reactor(Observable):

    def __init__(self):
        super().__init__()

        # Scheduling
        self._queue = queue.Queue()
        # To avoid busy waiting, wait this number of seconds if there is no
        # task or timeout to process in an iteration.
        self._WAIT_ON_EMPTY = .5
        self._timeouts = []
        self._cancellations = 0

        # Running
        self._thread = None
        self._is_dead = False
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

    def is_dead(self):
        return self._is_dead


    # Scheduler methods
    def schedule(self, event):
        if event.is_timeout():
            heapq.heappush(self._timeouts, event)
        else:
            self._queue.put(event)

    def remove_event(self, event):
        # If event is delayed, ie. is a timeout, than increase timeout
        # cancellations counter
        if event.is_timeout() is not None:
            self._cancellations += 1
        event.deactivate()

    def _process_timeouts(self):
        due_timeouts = []
        while self._timeouts:
            if self._timeouts[0].is_processed():
                heapq.heappop(self._timeouts)
                self._cancellations -= 1
            elif self._timeouts[0].deadline <= self.now:
                due_timeouts.append(heapq.heappop(self._timeouts))
            else:
                break
        if self._cancellations > 512 and \
           self._cancellations > (len(self._timeouts) >> 1):
                self._cancellations = 0
                self._timeouts = [x for x in self._timeouts if not x.is_processed()]
                heapq.heapify(self._timeouts)

        for timeout in due_timeouts:
            timeout.process()

    def _process_tasks(self, timeout):
        try:
            event = self._queue.get(timeout=timeout)
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

                time_to_nearest = self._WAIT_ON_EMPTY
                if self._timeouts:
                    time_to_nearest = max(0, self._timeouts[0].deadline - self.now)

                # This is where all the action happens.
                self._process_tasks(time_to_nearest)

                if self._timeouts:
                    self._process_timeouts()
        except:
# http://stackoverflow.com/questions/5191830/python-exception-logging#comment5837573_5191885
            logging.exception("Reactor has failed on exception!")

        self._is_dead = True
        self.notify("end", None)


class IOReactor(Reactor):
    """ IOReactor is an extension of Reactor, which can accept and send events
    to outside world through file descriptors. Internal implementation is based
    on 'select.epoll', therefore it only works on machines supporting epoll.
    """

    def __init__(self):
        Reactor.__init__(self)

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
