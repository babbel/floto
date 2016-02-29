import floto
from floto.decisions import Decision


class FailWorkflowExecution(Decision):
    def __init__(self, details=None, reason=None):
        super().__init__()
        self.details = details
        self.reason = reason

    def _get_decision(self):
        return {'decisionType': 'FailWorkflowExecution',
                'failWorkflowExecutionDecisionAttributes': self.decision_attributes()}

    def decision_attributes(self):
        a = {}
        if self.details:
            a['details'] = floto.specs.JSONEncoder.dump_object(self.details)

        if self.reason:
            a['reason'] = floto.specs.JSONEncoder.dump_object(self.reason)
        return a
