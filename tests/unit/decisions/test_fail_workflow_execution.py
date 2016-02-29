import pytest
import json
import floto.decisions

class TestFailWorkflowExecution(object):
    def test_init(self):
        d = floto.decisions.FailWorkflowExecution(details='something wrong', reason='failure')
        assert d.details == 'something wrong'
        assert d.reason == 'failure'

    def test_decision_attributes(self):
        d = floto.decisions.FailWorkflowExecution()
        d.details = {'failed_tasks':{'aid':{'details':'d_aid', 'reason':'r_aid'}}}
        d.reason = 'task_failed'

        attributes = d.decision_attributes()
        details = json.loads(attributes['details'])
        reason = attributes['reason']
        assert details['failed_tasks']['aid']['details'] == 'd_aid'
        assert reason == 'task_failed'

    def test_get_decision(self):
        d = floto.decisions.FailWorkflowExecution(details='something wrong', reason='failure')
        d_dict = d._get_decision()
        a = d_dict['failWorkflowExecutionDecisionAttributes']

        assert d_dict['decisionType'] == 'FailWorkflowExecution'
        assert a['reason'] == 'failure'
        




