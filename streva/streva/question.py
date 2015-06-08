
class Question:

    def __init__(self, questioner, ok, fail):
        self.respondents = set()
        self.questioner = questioner
        self.ok = self.fail = lambda _: None

    def clear(self):
        self.respondents = set()

    def pose(self, respondent, event_name, message, urgent=False, timeout=5.0):
        self.respondents.add(respondent)
        respondent.ask(self, questioner, event_name, message, self._expect_success, urgent)
        self._register_failure_timeout(respondent, timeout)

    def _expect_success(self, msg):
        if respondent in self.respondents:
            self.ok(respondent)
            self.respondents.remove(respondent)

    def _register_failure_timeout(self, respondent, timeout):
        def expect_failure(_):
            if respondent in self.respondents:
                self.fail(respondent)
                self.respondents.remove(respondent)

        questioner.add_timeout(expect_failure, timeout)
