
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

    def pose(self, respondent, event_name, message, urgent=False, timeout=5.0):
        self.respondents.add(respondent)
        respondent.ask(self.questioner, event_name, message, self._expect_success, urgent)
        self._register_failure_timeout(respondent, timeout)

    def _expect_success(self, msg):
        respondent = msg
        if respondent in self.respondents:
            self._ok(respondent)
            self.respondents.remove(respondent)

    def _register_failure_timeout(self, respondent, timeout):
        def expect_failure(_):
            if respondent in self.respondents:
                self._fail(respondent)
                self.respondents.remove(respondent)

        self.questioner.add_timeout(expect_failure, timeout)