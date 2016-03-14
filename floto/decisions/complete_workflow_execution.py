import json

from floto.decisions import Decision
import logging

logger = logging.getLogger(__name__)

class CompleteWorkflowExecution(Decision):
    def __init__(self, result=None):
        self.required_fields = ['decisionType']
        self.result = result

    def _get_decision(self):
        logger.debug('CompleteWorkflowExecution._get_decision..')
        d = {'decisionType': 'CompleteWorkflowExecution'}
        if self.result:
            d['completeWorkflowExecutionDecisionAttributes'] = {'result': json.dumps(self.result)}
        return d
