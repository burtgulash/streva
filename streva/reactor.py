#!/usr/bin/python3

import heapq
import functools
import logging
import os
import select
import threading
import time
import queue

from .stats import Stats



class Event:

    __slots__ = ["deadline", "delay", "callback", "message", "processed"]

    def __init__(self, callback, message, delay=None):
        self.callback = callback
        self.message = message
        self.processed = False

        self.delay = delay
        if delay is not None:
            self.deadline = time.time() + delay

    def process(self):
        if self.callback:
            self.callback(self.message)
        self.processed = True

    def deactivate(self):
        self.callback = None

    def __lt__(self, other):
        return self.deadline < other.deadline

    def __le__(self, other):
        return self.deadline <= other.deadline


class Reactor:
    """ Reactor class is an implementation of scheduled event loop.

    A reactor can be sent messages to, which are then in turn handled by
    component's handlers. Sleeping on the thread is implemented by timeouts,
    which are callbacks delayed on the reactor's scheduler calendar.
    """

    def __init__(self):

        # Scheduling
        self._queue = queue.Queue()
        # To avoid busy waiting, wait this number of seconds if there is no
        # task or timeout to process in an iteration.
        self._WAIT_ON_EMPTY = .5
        self._timeouts = []
        self._cancellations = 0

        # Handlers
        self._observers = {}

        # Running
        self._thread = None
        self._is_dead = False
        self._should_run = True

    # Lifecycle methods
    def stop(self):
        self._should_run = False

        # Flush the queue with empty message if it was waiting for a timeout
        self.schedule(lambda msg: None, None)
        self._thread.join()

    def start(self):
        self._thread = threading.Thread(target=self._run)
        self._thread.start()

    def is_dead(self):
        return self._is_dead


    # Lifecycle notifications methods
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


    # Scheduler methods
    def schedule(self, callback, message, delay=None):
        if delay:
            timeout = Event(callback, message, delay=delay)
            heapq.heappush(self._timeouts, timeout)
            return timeout
        else:
            event = Event(callback, message)
            self._queue.put(event)
            return event

    def remove_event(self, event):
        # If event is delayed, ie. is a timeout, than increase timeout
        # cancellations counter
        if event.delay is not None:
            self._cancellations += 1
        event.callback = None

    def _process_event(self, event, ok="processed", err="processing_error"):
        # Make 'error' variable, because if the error notification was in
        # except clause, it would print double exceptions. Something like:
        # Exception happened... during exception another exception happened...
        error = None
        try:
            event.process()
        except Exception as e:
            error = e

        if error is None:
            self.notify(ok, event)
        else:
            self.notify(err, (event, error))

    def _process_task(self, task):
        self._process_event(task, ok="task_processed", err="processing_error")

    def _process_timeout(self, timeout):
        self._process_event(timeout, ok="timeout_processed", err="processing_error")

    def _process_timeouts(self):
        due_timeouts = []
        while self._timeouts:
            if self._timeouts[0].callback is None:
                heapq.heappop(self._timeouts)
                self._cancellations -= 1
            elif self._timeouts[0].deadline <= self.now:
                due_timeouts.append(heapq.heappop(self._timeouts))
            else:
                break
        if self._cancellations > 512 and \
           self._cancellations > (len(self._timeouts) >> 1):
                self._cancellations = 0
                self._timeouts = [x for x in self._timeouts
                                  if x.callback is not None]
                heapq.heapify(self._timeouts)

        for timeout in due_timeouts:
            self._process_timeout(timeout)

    def _process_tasks(self, timeout):
        """ Process events from component's queue.
        Return True if timeouted, False otherwise.
        """

        try:
            event = self._queue.get(timeout=timeout)
        except queue.Empty:
            # Timeout obtained means that a timeout event came before an event
            # from the queue
            pass
        else:
            self._process_task(event)

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
            logging.exception("Component failed on exception!")

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

    def schedule(self, callback, message, delay=None):
        # Schedule the event (put task event into queue)
        event = Reactor.schedule(self, callback, message, delay=delay)

        if not delay:
            # Signal about task event to epoll by sending a random single byte
            # to it
            os.write(self._inside_events_pipe[1], b'X')

        return event



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
                self._process_task(event)
            else:
                self.process_poll_event(fd, event)


    def process_poll_event(self, fd, event):
        raise NotImplementedError("process_poll_event must be overriden")

