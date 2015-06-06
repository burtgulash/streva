
class Observable:

    def __init__(self):
        self._observers = {}

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
