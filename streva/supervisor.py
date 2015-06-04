
import logging
import signal


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


class SupervisorMixin:

    def __init__(self, probe_period):
        self._supervised_actors = set()
        self._ping_questions = {}

        # Probe all supervised actors regularly with this time period in
        # seconds
        self._probe_period = probe_period

        # Echo timeouts after this many seconds. Ie. this means the time period
        # after which a supervised actor should be recognized as dead
        self._failure_timeout_period = 10

        # Make sure that all actors are evaluated of one round of probing
        # before next round of probing
        assert self._failure_timeout_perido * 2 < self._probe_period

        self.add_handler("_error", self.error_received)
        self.add_handler("_pong", self._receive_pong)

    # Not responding and error handlers
    def not_responding(self, actor):
        name = actor.name or str(id(actor))
        logging.error("Actor '{}' hasn't responded in {} seconds!".format(name, 
                                                                    self._failure_timeout_period))

    def error_received(self, error_message):
        errored_event, error = error_message


    # Supervisor processes
    def init(self, msg):
        super().init(self, msg)       

        def probe(_):
            # Reset ping questions
            self._ping_questions = {}

            for actor in self._supervised_actors:
                # Notice the message 'self'. Echoed actor uses that as a
                # recipient for this ping response
                actor.send("_ping", self)

                # Register timeout for this ping question
                self._register_failure_timeout(actor)

            # Loop pattern: repeat probing process
            self.add_timeout(probe, self._probe_period)
        self.add_timeout(probe, self._probe_period)


    def _register_failure_timeout(self, actor):
        def failure_timeout(_):
            # Failure timeout received for an actor, because it didn't respond
            # in time.
            if actor in self._ping_questions:
                del self._ping_questions[actor]

                self.not_responding(actor)

        self.add_timeout(failure_timeout, self._failure_timeout_period)

    def _receive_pong(self, msg):
        sender = msg

        # Failure timeout not yet received. Simply remove the question to
        # denote a success
        if sender in self._ping_questions:
            del self._ping_questions[sender]




        




