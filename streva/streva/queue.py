from collections import deque
from threading import Condition
import time


class Empty(Exception):
    pass


class Queue:

    def __init__(self):
        self.q = deque()
        self.not_empty = Condition()

    def enqueue(self, x, prioritized=False):
        with self.not_empty:
            if prioritized:
                self.q.appendleft(x)
            else:
                self.q.append(x)
            self.not_empty.notify()

    def dequeue(self, block=True, timeout=None):
        with self.not_empty:
            if not block:
                if len(self.q) == 0:
                    raise Empty
            elif timeout is None:
                if len(self.q) == 0:
                    self.not_empty.wait()
            elif timeout < 0:
                raise ValueError("'timeout' must be a non-negative number.")
            else:
                endtime = time.time() + timeout
                while len(self.q) == 0:
                    remaining = endtime - time.time()
                    if remaining < 0.0:
                        raise Empty
                    self.not_empty.wait(remaining)
            return self.q.popleft()

    def get_no_wait(self):
        self.dequeue(block=False)

    

