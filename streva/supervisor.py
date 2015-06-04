
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






        




