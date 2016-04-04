from floto.decisions import Decision


class StartTimer(Decision):
    def __init__(self, timer_id, start_to_fire_timeout):
        super().__init__(required_fields=['decisionType',
                                          'startTimerDecisionAttributes.startToFireTimeout',
                                          'startTimerDecisionAttributes.timerId'])
        self.timer_id = timer_id
        self.start_to_fire_timeout = start_to_fire_timeout

    def _get_decision(self):
        return {'decisionType': 'StartTimer',
                'startTimerDecisionAttributes': self.decision_attributes()}

    def decision_attributes(self):
        attributes = {'startToFireTimeout': str(self.start_to_fire_timeout),
                      'timerId': self.timer_id}
        return attributes
