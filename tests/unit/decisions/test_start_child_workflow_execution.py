import pytest
import floto.decisions
import floto.api

import json

class TestStartChildWorkflowExecution():
    def test_init_workflow_id(self):
        w = floto.decisions.StartChildWorkflowExecution(workflow_id='id') 
        assert w.get_attributes()['workflowId'] == 'id'
        
    def test_init_workflow_type(self):
        workflow_type = floto.api.WorkflowType(name='wft_name', version='wft_version')
        w = floto.decisions.StartChildWorkflowExecution(workflow_type=workflow_type) 
        assert w.get_attributes()['workflowType'] == {'name':'wft_name', 'version':'wft_version'}
        
    def test_init_task_list(self):
        w = floto.decisions.StartChildWorkflowExecution(task_list='tl') 
        assert w.get_attributes()['taskList'] == {'name': 'tl'}
        
    def test_init_input(self):
        w = floto.decisions.StartChildWorkflowExecution(input={'foo':'bar'}) 
        assert w.get_attributes()['input'] == {'foo':'bar'} 

    def test_property_workflow_id(self):
        w = floto.decisions.StartChildWorkflowExecution() 
        w.workflow_id = 'id'
        assert w.workflow_id == 'id'

    def test_property_task_list(self):
        w = floto.decisions.StartChildWorkflowExecution() 
        w.task_list = 'tl'
        assert w.task_list == 'tl'

    def test_property_workflow_type(self):
        w = floto.decisions.StartChildWorkflowExecution() 
        w.workflow_type = floto.api.WorkflowType(name='n', version='v') 
        assert w.workflow_type == {'name':'n', 'version':'v'} 

    #def test_from_json(self):
        #j = '{"workflow_id": "my_child_workflow"}'
        #w = floto.decisions.StartChildWorkflowExecution() 
        #w.from_json(j)
        #assert w.workflow_id == 'my_child_workflow'

    #def test_json_serializablity(self):
        #j = """{"startChildWorkflowExecutionDecisionAttributes":{
                  #"workflowId":"my_id", 
                  #"workflowType":{"name":"wft_name", "version":"wft_version"}}}"""
        #w = floto.decisions.StartChildWorkflowExecution()
        #w.from_json(j)
        #d = w.get_decision()
        #attributes = d['startChildWorkflowExecutionDecisionAttributes']
        #assert d['decisionType'] == 'StartChildWorkflowExecution'
        #assert attributes['workflowId'] == 'my_id' 
        #assert attributes['workflowType']['name'] == 'wft_name' 


