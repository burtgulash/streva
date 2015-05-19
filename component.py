
class Component:
    
    def __init__(self):
        self._should_run = True
        self._events = queue.Queue()
        self._timeouts = []

        self._run()

    def on_start(self):
        """ Override """
        pass

    def on_end(self):
        """ Override """
        pass

    def on_after_task(self):
        """ Override """
        pass

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
        if self._cancellations > 512 and
            self._cancellations > (len(self._timeouts) >> 1):
                self._cancellations = 0
                self._timeouts = [x for x in self._timeouts 
                                  if x.callback is not None]
                heapq.heapify(self._timeouts)

        for timeout in due_timeouts:
            if timeout.callback is not None:
                self._run_callback(timeout.callback)

    def call_later(self, delay, callback, *args, **kwargs):
        return self.call_at(time.time() + delay)

    def call_at(self, deadline, callback, *args, **kwargs):
        timeout = _Timeout(deadline, functools.partial(callback, args, kwargs))
        heapq.heappush(self._timeouts, timeout)
        return timeout

    def remove_timeout(self, timeout):
        timeout.callback = None
        self._cancellations += 1
            

    def _run(self):
        self.on_start()

        while self._should_run:
            self.now = time.time()

            time_to_nearest = -1
            if self._timeouts:
                time_to_nearest = self._timeouts[0].deadline - self.now

            try:
                task = self._tasks.get(timeout=time_to_nearest)
            except queue.Empty:
                self._process_timeouts()
            else:
                self._process_task()

            self.on_after_task()

        self.on_end()


    class _Timeout:

        __slots__ = ["deadline", "callback"]

        def __init__(self, deadline, callback):
            self.deadline = deadline
            self.callback = callback

        def __lt__(self, other):
            return self.deadline < other.deadline

        def __le__(self, other):
            return self.deadline <= other.deadline

