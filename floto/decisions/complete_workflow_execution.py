import json

from floto.decisions import Decision


class CompleteWorkflowExecution(Decision):
    def __init__(self, result=None):
        self.required_fields = ['decisionType']
        self.result = result

    def _get_decision(self):
        d = {'decisionType': 'CompleteWorkflowExecution'}
        if self.result:
            d['completeWorkflowExecutionDecisionAttributes'] = {'result': json.dumps(self.result)}
        return d
