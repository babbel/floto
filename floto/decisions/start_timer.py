from floto.decisions import Decision


class StartTimer(Decision):
    def __init__(self, timer_id, start_to_fire_timeout):
        self.timer_id = timer_id
        self.start_to_fire_timeout = start_to_fire_timeout

        self.required_fields = ['decisionType',
                                'startTimerDecisionAttributes.startToFireTimeout',
                                'startTimerDecisionAttributes.timerId']

    def _get_decision(self):
        return {'decisionType': 'StartTimer',
                'startTimerDecisionAttributes': self.decision_attributes()}

    def decision_attributes(self):
        attributes = {}
        attributes['startToFireTimeout'] = str(self.start_to_fire_timeout)
        attributes['timerId'] = self.timer_id
        return attributes
