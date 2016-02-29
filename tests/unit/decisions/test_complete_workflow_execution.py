import pytest
import json
from floto.decisions import CompleteWorkflowExecution

class TestCompleteWorkflow():
    def test_get_decision(self):
        d = CompleteWorkflowExecution().get_decision()
        assert d['decisionType'] == 'CompleteWorkflowExecution'
        
    def test_get_decision_with_result(self):
        d = CompleteWorkflowExecution(result={'foo':'bar'}).get_decision()
        expected_result = json.dumps({'foo':'bar'})
        assert d['completeWorkflowExecutionDecisionAttributes']['result'] == expected_result
        
