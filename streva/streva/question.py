
class Questionnaire:

    def __init__(self, questioner, ok=None, fail=None):
        self.respondents = set()
        self.questioner = questioner

        self._ok = self._fail = lambda _: None
        if ok:
            self._ok = ok
        if fail:
            self._fail = fail

    def is_empty(self):
        return len(self.respondents) == 0

    def clear(self):
        self.respondents = set()

    def pose(self, respondent, event_name, message, timeout, urgent=False):
        self.respondents.add(respondent)
        respondent.send(event_name, message, respond=(self.questioner, self._response_callback), urgent=urgent)
        self._register_failure_timeout(respondent, timeout)

    def _response_callback(self, msg):
        respondent = msg
        if respondent in self.respondents:
            self.respondents.remove(respondent)
            self._ok(respondent)

    def _register_failure_timeout(self, respondent, timeout):
        def expect_failure(_):
            if respondent in self.respondents:
                self.respondents.remove(respondent)
                self._fail(respondent)

        self.questioner.add_timeout(expect_failure, timeout)
