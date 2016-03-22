import logging

import floto
from floto.decisions import Decision

logger = logging.getLogger(__name__)

class FailWorkflowExecution(Decision):
    def __init__(self, details=None, reason=None):
        super().__init__()
        self.details = details
        self.reason = reason

    def _get_decision(self):
        return {'decisionType': 'FailWorkflowExecution',
                'failWorkflowExecutionDecisionAttributes': self.decision_attributes()}

    def decision_attributes(self):
        logger.debug('FailWorkflowExecution.decision_attributes...')
        a = {}
        if self.details:
            a['details'] = floto.specs.JSONEncoder.dump_object(self.details)

        if self.reason:
            a['reason'] = floto.specs.JSONEncoder.dump_object(self.reason)
        return a
