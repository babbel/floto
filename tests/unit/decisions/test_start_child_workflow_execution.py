import pytest
import floto.decisions
import floto.api

import json

class TestStartChildWorkflowExecution():
    def test_init_workflow_id(self):
        w = floto.decisions.StartChildWorkflowExecution(workflow_id='id') 
        assert w.get_attributes()['workflowId'] == 'id'
        
    def test_init_workflow_type(self):
        workflow_type = floto.api.WorkflowType(domain='d', name='wft_name', version='wft_version')
        w = floto.decisions.StartChildWorkflowExecution(workflow_type=workflow_type) 
        assert w.get_attributes()['workflowType'] == {'name':'wft_name', 'version':'wft_version'}
        
    def test_init_task_list(self):
        w = floto.decisions.StartChildWorkflowExecution(task_list='tl') 
        assert w.get_attributes()['taskList'] == {'name': 'tl'}
        
    def test_init_input(self):
        w = floto.decisions.StartChildWorkflowExecution(input={'foo':'bar'}) 
        assert json.loads(w.get_attributes()['input']) == {'foo':'bar'} 

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
        w.workflow_type = floto.api.WorkflowType(domain='d', name='n', version='v')
        assert w.workflow_type == {'name':'n', 'version':'v'} 


