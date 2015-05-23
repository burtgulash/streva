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


class Reactor:
    """ Reactor class is an implementation of scheduled event loop.

    A reactor can be sent messages to, which are then in turn handled by
    component's handlers. Sleeping on the thread is implemented by timeouts,
    which are callbacks delayed on the reactor's scheduler calendar.
    """

    def __init__(self):

        # Scheduling
        self._tasks = queue.Queue()
        # To avoid busy waiting, wait this number of seconds if there is no
        # task or timeout to process in an iteration.
        self._WAIT_ON_EMPTY = .5
        self._timeouts = []
        self._cancellations = 0

        # Handlers
        self._handlers = {}

        # Running
        self._thread = None
        self._is_dead = False
        self._should_run = True

    def send(self, event_name, message):
        """ Send message to this reactor registered by event_name.
        """
        self._tasks.put((event_name, message))

    def stop(self):
        self._should_run = False
        self._thread.join()

    def start(self):
        self._thread = threading.Thread(target=self._run)
        self._thread.start()

    def is_dead(self):
        return self._is_dead


    def add_handler(self, event_name, callback):
        if event_name not in self._handlers:
            self._handlers[event_name] = []
        self._handlers[event_name].append(callback)

    def del_handler(self, event_name, callback):
        if event_name in self._handlers:
            without_observer = []
            for observer in self._handlers[event_name]:
                if observer != calback:
                    without_observer.append(observer)
            self._handlers[event_name] = without_observer

    def _process_event(self, event_name, message):
        if event_name in self._handlers:
            for handler in self._handlers[event_name]:
                handler(message)


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

        no_processed = len(due_timeouts)
        for timeout in due_timeouts:
            if timeout.callback is not None:
                timeout.callback()

        return no_processed


    def call_later(self, delay, callback, *args, **kwargs):
        return self.call_at(time.time() + delay, callback, *args, **kwargs)

    def call_at(self, deadline, callback, *args, **kwargs):
        timeout = self._Timeout(deadline, functools.partial(callback, *args, **kwargs))
        heapq.heappush(self._timeouts, timeout)
        return timeout

    def remove_timeout(self, timeout):
        timeout.callback = None
        self._cancellations += 1

    def _loop_iteration(self):
        self.now = time.time()

        time_to_nearest = self._WAIT_ON_EMPTY
        if self._timeouts:
            time_to_nearest = max(0, self._timeouts[0].deadline - self.now)

        # This is where all the action happens.
        has_timeouted = self._process_events(time_to_nearest)

        if self._timeouts:
            self._process_timeouts()


    def _run(self):
        self.send("start", None)

        try:
            while self._should_run:
                self._loop_iteration()
        except:
# http://stackoverflow.com/questions/5191830/python-exception-logging#comment5837573_5191885
            logging.exception("Component failed on exception!")

        self._is_dead = True
        self.send("end", None)

    def _process_events(self, timeout):
        """ Process events from component's queue.
        Return True if timeouted, False otherwise.
        """

        try:
            handler_name, message = self._tasks.get(timeout=timeout)
        except queue.Empty:
            # Timeout obtained means that a timeout event came before an event
            # from the queue
            return True
        else:
            self._process_event(handler_name, message)
            return False


    class _Timeout:

        __slots__ = ["deadline", "callback"]

        def __init__(self, deadline, callback):
            self.deadline = deadline
            self.callback = callback

        def __lt__(self, other):
            return self.deadline < other.deadline

        def __le__(self, other):
            return self.deadline <= other.deadline


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

    def send(self, event_name, message):
        # Put task message into queue
        Reactor.send(self, event_name, message)
        # Signal about this event to epoll by sending a random single byte to it
        os.write(self._inside_events_pipe[1], b'X')

    def _process_events(self, timeout):
        events = self._poll.poll(timeout)
        if not events:
            # No events -> timeout happened
            return True

        for fd, event in events:
            if fd == self._inside_events_pipe[0]:
                # Consume the '\0' byte sent by 'send' method and process the event.
                os.read(fd, 1)
                event_name, message = self._tasks.get_nowait()
                self._process_event(event_name, message)
            else:
                self.process_poll_event(fd, event)

        return False

    def process_poll_event(self, fd, event):
        raise NotImplementedError("process_poll_event must be overriden")



class MonitoredReactor(Reactor):
    """ Monitored reactor collects runtime data of a reactor loop and makes it
    available in its 'stats' field.
    """

    def __init__(self):
        Reactor.__init__(self)

        # Monitoring
        self.stats = Stats()
        self.stats.register_operation_stats("timeouts")

    def add_handler(self, operation_name, handler):
        Reactor.add_handler(self, operation_name, handler)

        self.stats.register_operation_stats(operation_name)

    def _process_event(self, operation_name, message):
        Reactor._process_event(self, operation_name, message)

        running_time = time.time() - self.now
        self.stats.update_running_stats(operation_name, running_time, 1)

    def _process_timeouts(self):
        no_processed = Reactor._process_timeouts(self)

        running_time = time.time() - self.now
        self.stats.update_running_stats("timeouts", running_time, no_processed)

    def _loop_iteration(self):
        Reactor._loop_iteration(self)

        self.stats.queue_size = self._tasks.qsize()

