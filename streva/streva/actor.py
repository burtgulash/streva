from functools import wraps
import logging
import threading
import time
import traceback


from streva.reactor import Reactor, TimedReactor


class ErrorContext:

    def __init__(self, process, operation, message, error):
        self.process = process
        self.operation = operation
        self.message = message
        self.error = error

    def get_exception(self):
        return self.error

    def _exc_traceback(self):
        error = self.error
        exc_info = type(error), error, error.__traceback__
        return "".join(traceback.format_exception(*exc_info))

    def __str__(self):
        return """

ERROR happened when process  '{}.{}'  was sent message  '{}':
{}""".format(self.process.name, self.operation,
             str(self.message)[:30],
             self._exc_traceback())


class Cancellable:

    __slots__ = "f", "cleanup", "cancelled", "finished"

    nop = lambda: None

    def __init__(self, f):
        self.f = f
        self.cleanup = self.nop
        self.cancelled = False
        self.finished = False

    def __call__(self):
        if not self.cancelled:
            self.f()
        self.cleanup()
        self.finished = True

    def __repr__(self):
        return "Cancellable({})".format(self.f)

    def add_cleanup_f(self, cleanup_f):
        self.cleanup = cleanup_f

    def cancel(self):
        self.finished = True

    def is_finished(self):
        return self.finished

    def is_canceled(self):
        return self.cancelled


class Enablable:

    def __init__(self):
        self.__active = False
        self.__waiting = []

    def __process_waiting(self):
        for message in self.__waiting:
            self._process(message)
        self.__waiting = []

    def _process(self, message):
        pass

    def _enqueue(self, message):
        self.__waiting.append(message)

    def active(self):
        return self.__active

    def activate(self):
        self.__active = True
        self.__process_waiting()

    def flush(self):
        self.__waiting = []

    def deactivate(self):
        self.__active = False


class ProcessBase(Enablable):

    def __init__(self):
        super().__init__()
        self.__reactor = None
        self.__planned = set()
        # Deliberately start the Process as 'not __stopped'
        self.__stopped = False

    def get_reactor(self):
        return self.__reactor

    def set_reactor(self, reactor):
        self.__reactor = reactor
        self.activate()

    def unset_reactor(self):
        self.deactivate()
        self.__reactor = None

    def _process(self, message):
        if not self.__reactor:
            raise ValueError("Loop must be set before starting the process.!")

        function, when = message
        self.__planned.add(function)
        self.__reactor.receive(function, when)

    def terminate(self):
        pass

    def flush(self):
        super().flush()
        for f in self.__planned:
            f.cancel()

    def stop(self):
        self.call(self._stop)

    def start(self):
        self.__stopped = False

    def _stop(self):
        self.flush()
        self.terminate()
        self.__stopped = True

    def call(self, function, *args, when=Reactor.NOW, **kwds):
        @wraps(function)
        def baked():
            function(*args, **kwds)

        func = Cancellable(baked)

        def cleanup():
            self.__planned.remove(func)
        func.add_cleanup_f(cleanup)

        if not self.__stopped:
            if self.active():
                self._process((func, when))
            else:
                self._enqueue((func, when))


class Port(Enablable):
    """ Port is a named set of processes to all of which an outbound message will
    be sent through this port.

    Port implements pubsub routing.
    """

    def __init__(self, name):
        super().__init__()
        self.name = name
        self.__targets = []

    def add_target(self, process, operation):
        self.__targets.append((process, operation))

    def _process(self, message):
        for target_process, operation in self.__targets:
            target_process.send(operation, message)

    def send(self, message):
        if self.active():
            self._process(message)
        else:
            self._enqueue(message)


class ProcessMeta(type):

    @staticmethod
    def handler_deco(handlers):
        def bake_in(operation):
            def decorator(func):
                handlers.append((operation, func))
                return func
            return decorator
        return bake_in

    # Register 'handler_for' decorator and make it put all recognized handlers
    # to class attribute "process_handlers". Handlers will then be registered in
    # __init__ of each class when even overriden methods are known.
    def __prepare__(name, bases):
        process_handlers = []

        for base in bases:
            if hasattr(base, "_process_handlers"):
                for handler_pair in base._process_handlers:
                    process_handlers.append(handler_pair)

        handler_for = ProcessMeta.handler_deco(process_handlers)

        return {"_process_handlers": process_handlers,
                "handler_for": handler_for,
                "cls_name": name,
                "_ids": 0}


class Process(ProcessBase, metaclass=ProcessMeta):

    def __init__(self):
        super().__init__()
        self.name = "{} #{}".format(self.cls_name, self.__class__._ids)
        self.__class__._ids += 1

        self.__handlers = {}
        self.__ports = {}

        for operation, func in self._process_handlers:
            # Preferably get overriden method by name from instance
            method = getattr(self, func.__name__, None)

            # Method is private, turn function into method
            if not method:
                method = func.__get__(self, type(self))

            self.add_handler(operation, method)

    def set_name(self, name):
        self.name = name

    def add_handler(self, operation, handler):
        if operation in self.__handlers:
            raise ValueError("Handler for '{}' already exists!".format(operation))
        self.__handlers[operation] = handler

    def add_port(self, port_name, port):
        self.__ports[port_name] = port

    def make_port(self, port_name):
        port = Port(port_name)
        self.add_port(port_name, port)
        return port

    def start(self):
        for port in self.__ports.values():
            port.activate()
        super().start()

    def connect(self, port_name, to_process, to_operation):
        port = self.__ports[port_name]
        port.add_target(to_process, to_operation)

    def _add_callback(self, operation, function, message, when=Reactor.NOW):
        self.call(function, message, when=when)

    def send(self, operation, message, respond=None):
        handler = self.__handlers[operation]

        f = handler
        if respond is not None:
            sender, respond_operation = respond

            @wraps(handler)
            def resp_wrap(msg):
                handler(msg)
                sender.send(respond_operation, self)
            f = resp_wrap

        self._add_callback(operation, f, message)


class Intercepted(Process):

    def __init__(self):
        super().__init__()
        self.__intercept_lock = threading.Lock()

    class Id:
        pass

    def before_schedule(self, execution_id, operation, function, message):
        pass

    def before_execute(self, execution_id, operation, function, message):
        pass

    def after_execute(self, execution_id, operation, function, message):
        pass

    def _add_callback(self, operation, function, message, when=Reactor.NOW):
        execution_id = id(self.Id())

        # Because _add_callback is the only function, which is called from
        # another thread and method self.before_schedule can modify this
        # process's state, we need to put it into critical section
        with self.__intercept_lock:
            self.before_schedule(execution_id, operation, function, message)

        @wraps(function)
        def intercepted_function(message):
            self.before_execute(execution_id, operation, function, message)
            function(message)
            self.after_execute(execution_id, operation, function, message)

        super()._add_callback(operation, intercepted_function, message, when=when)


class Timer(Process):

    def set_reactor(self, reactor):
        if not isinstance(reactor, TimedReactor):
            raise TypeError("Loop for Timer must be TimedLoop instance!")
        super().set_reactor(reactor)

    def register_timer(self, to_process):
        timer_port = to_process.make_port("_my_timer")
        to_process.connect("_my_timer", self, "_after")
        return timer_port

    def add_timeout(self, callback, after, message=None):
        self._add_callback("_timeout", callback, message, when=after)

    @handler_for("_after")
    def __after(self, msg):
        sender, operation, after = msg
        self.add_timeout(self.__delayed_send, after, message=(sender, operation))

    def __delayed_send(self, msg):
        sender, operation = msg
        sender.send(operation, None)


class Stats:

    class StatType:

        def __init__(self, type_name):
            self.type_name = type_name
            self.value = 0

        def avg(self, runs):
            return self.value / runs if runs else 0

        def add(self, value):
            self.value += value



    def __init__(self, operation):
        self.operation = operation
        self.runs = 0
        self.processing_time = self.StatType("processing time")
        self.waiting_time = self.StatType("waiting time")
        self.total_time = self.StatType("total time")

        self.time_types = self.processing_time, self.waiting_time, self.total_time

    def __str__(self):
        s = ["    {:22} {:.4f}[s] / {} = {:.4f}[s]".format(x.type_name.capitalize(),
                                                           x.value,
                                                           self.runs,
                                                           x.avg(self.runs))
             for x
             in self.time_types]

        return "+ {}\n{}".format(self.operation, "\n".join(s))

    def add(self, other):
        self.runs += other.runs
        for a, b in zip(self.time_types, other.time_types):
            a.add(b.value)


class Measured(Intercepted, Process):

    class Execution:

        def __init__(self):
            self.planned_at = None
            self.started_at = None

    def __init__(self):
        super().__init__()

        self.last_updated = time.time()
        self.__stats = {}
        self.__intercepted_events = {}

    def before_schedule(self, execution_id, operation, function, message):
        if operation not in self.__stats:
            self.__stats[operation] = Stats(operation)

        execution = self.Execution()
        execution.planned_at = time.time()

        self.__intercepted_events[execution_id] = execution

    def before_execute(self, execution_id, operation, function, message):
        self.__intercepted_events[execution_id].started_at = time.time()

    def after_execute(self, execution_id, operation, function, message):
        assert operation in self.__stats
        assert execution_id in self.__intercepted_events

        now = time.time()

        stats = self.__stats[operation]
        execution = self.__intercepted_events[execution_id]

        stats.runs += 1
        stats.processing_time.add(now - execution.started_at)
        stats.total_time.add(now - execution.planned_at)
        stats.waiting_time.value = stats.total_time.value - stats.processing_time.value

        self.last_updated = now

    def get_stats(self):
        return [s for s in sorted(self.__stats.values(), key=lambda x: x.runs)]

    def get_total_stats(self):
        total = Stats("Total")
        for stats in self.get_stats():
            total.add(stats)
        return total

    def print_stats(self):
        print("\n# STATS for process '{}':".format(self.name))
        print("sorted by number of runs. (total time[s]/runs = avg time[s])")
        for stats in self.get_stats():
            print(stats)
        print(self.get_total_stats())


class Monitored(Process):

    @handler_for("_ping")
    def __on_ping(self, msg):
        sender, operation = msg
        sender.send(operation, self)


class Supervised(Process):

    def __init__(self):
        super().__init__()
        self.supervisor = None
        self.__error_out = self.make_port("_error")

    def set_supervisor(self, supervisor):
        self.supervisor = supervisor

    def get_supervisor(self):
        return self.supervisor

    def is_supervised(self):
        return bool(self.supervisor)

    def on_error(self, err):
        # If there is no supervisor attached, then don't just pass the error
        # but raise it
        if not self.is_supervised():
            raise err

    def _add_callback(self, operation, function, message, when=Reactor.NOW):
        @wraps(function)
        def try_function(message):
            error = None
            try:
                function(message)
            except Exception as err:
                error = err

            if error is not None:
                error_context = ErrorContext(self, operation, message, error)
                self.__error_out.send(error_context)
                self.on_error(error)

        super()._add_callback(operation, try_function, message, when=when)

    @handler_for("_stop")
    def __on_stop(self, msg):
        sender, operation = msg
        self.stop()
        sender.send(operation, self)


class Monitor(Process):

    def __init__(self, timer, probe_period=30, timeout_period=10):
        super().__init__()
        self.timer = timer
        self.__monitored_processes = set()

        self.__ping_q = set()

        # Probe all supervised actors regularly with this time period in
        # seconds
        self.__probe_period = probe_period

        # Echo timeouts after this many seconds. Ie. this means the time period
        # after which a supervised process should be recognized as dead
        self.__failure_timeout_period = timeout_period

        # Make sure that all processes are evaluated of one round of probing
        # before next round of probing
        if not self.__failure_timeout_period * 2 < self.__probe_period:
            raise Exception("Timeout_period should be at most half the period of probe_period.")

    def monitor(self, process):
        self.__monitored_processes.add(process)

    def get_monitored(self):
        return iter(self.__monitored_processes)

    def not_responding(self, process):
        raise Exception("Process '{}' hasn't responded in {} seconds!".format(process.name,
                                                                    self.__failure_timeout_period))

    @handler_for("_probe")
    def _probe(self, _):
        self.__ping_q = set()

        for process in self.get_monitored():
            self.__ping_q.add(process)
            process.send("_ping", (self, "_pong"))

        self.timer.send((self, "_probe", self.__probe_period))

    @handler_for("_pong")
    def _pong(self, process):
        if process in self.__ping_q:
            self.__ping_q.remove(process)

    @handler_for("_ping_check_failures")
    def ping_check_failures(self, _):
        for process in self.__ping_q:
            self.__ping_q.remove(process)
            self.not_responding(process)


class Supervisor(Process):

    def __init__(self, timer):
        super().__init__()
        self.__supervised_processes = set()
        self.__stop_q = set()

        self.STOP_FAILED_AFTER = 5.0

        self.timer = timer
        self.timer.send((self, "_probe", self.__probe_period))

    def spawn(actor):
        self.supervise(actor)
        actor.start()

    def supervise(self, process):
        if not isinstance(process, Supervised):
            raise Exception("For the process '{}' to be supervised, add Supervised to its base classes".
                    format(process.name))

        self.__supervised_processes.add(process)
        process.set_supervisor(self)
        process.connect("_error", self, "_error")

    def get_supervised(self):
        return iter(self.__supervised_processes)

    @handler_for("_error")
    def error_received(self, error_context):
        process, error = error_context.process, error_context.get_exception()
        raise error

    def start(self):
        super().start()
        for process in self.get_supervised():
            process.start()

    def stop(self):
        self.stop_children()

    def all_stopped(self):
        super().stop()

    # Stopping
    def stop_children(self):
        self.timer.send((self, "_stop_check_failures", self.STOP_FAILED_AFTER))
        for process in self.get_supervised():
            self.__stop_q.add(process)
            process.send("_stop", (self, "_stop_received"))

    @handler_for("_stop_received")
    def _stop_received(self, process):
        if process in self.__stop_q:
            self.__stop_q.remove(process)
            if len(self.__stop_q) == 0:
                self.all_stopped()

    @handler_for("_stop_check_failures")
    def stop_check_failures(self, _):
        if len(self.__stop_q) > 0:
            self.__stop_q = set()
            logging.warning("""Killing everything ungracefully!
{} processes haven't responded to stop request in {} s.""".format(len(self.__stop_q),
                                                               self.__failure_timeout_period))
        else:
            self.all_stopped()



class Actor(Monitored, Supervisor, Supervised, Process):

    def __init__(self):
        Process.__init__()
        Supervised.__init__()

        self.timer = timer.register_timer(self)
        Supervisor.__init__(self.timer)
        
        Monitored.__init__()

    def timer(self):
        return self.timer


class Root(Monitor, Supervisor, Timer, Process):

    def __init__(self):
        Process.__init__()

        Timer.__init__()
        self.timer = self.register_timer(self)

        Supervisor.__init__(self.timer)
        Monitor.__init__(self.timer)

