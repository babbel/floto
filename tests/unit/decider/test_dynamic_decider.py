import pytest
import floto.decider
from floto.specs.task import ActivityTask
from floto.specs import DeciderSpec 

@pytest.fixture
def task():
    return ActivityTask(domain='d', name='activity2', version='v1')

@pytest.fixture
def decider_spec(task):
    activity_tasks = [task]
    decider_spec = DeciderSpec(domain='d', task_list='tl', activity_tasks=activity_tasks,
            terminate_decider_after_completion=True)
    return decider_spec

@pytest.fixture
def history(init_response):
    return floto.History(domain='d', task_list='tl', response=init_response)

@pytest.fixture
def decider(decider_spec, history):
    d = floto.decider.DynamicDecider(decider_spec, identity='d_id')
    d.history = history
    return d

class TestDynamicDecider:
    def test_init(self, decider, decider_spec):
        assert decider.decider_spec == decider_spec
        assert decider.identity == 'd_id'

    def test_get_decisions(self, decider, mocker, task):
        mocker.patch('floto.History.get_workflow_input', return_value='wf_input')
        mocker.patch('floto.decider.DynamicDecider.get_activity_tasks_from_input', 
                return_value=[task])
        mocker.patch('floto.decider.DecisionBuilder.get_decisions')
        mocker.patch('floto.decider.DecisionBuilder.is_terminate_workflow', return_value=True)
        decider.get_decisions()
        decider.get_activity_tasks_from_input.assert_called_once_with('wf_input')
        assert decider.terminate_workflow

    def test_activity_tasks_from_input(self, decider):
        d =  {'activity_tasks': [{'id_': 'fileLength:1:2be88ca424:d',
                                          'name': 'fileLength',
                                          'domain': 'd',
                                          'type': 'floto.specs.task.ActivityTask',
                                          'version': '1'}]}
        t = (decider.get_activity_tasks_from_input(d))
        assert isinstance(t[0], floto.specs.task.ActivityTask)
        assert t[0].name == 'fileLength'
                
    def test_activity_tasks_from_input_child_workflow(self, decider):
        d = {'copyFiles:1:15fbe8eeaa': ['/path/to/data/2016-03-06.json'],
             'child_workflow_task': {'activity_tasks': [{'id_': 'fileLength:1:2be88ca424:d',
                                             'name': 'fileLength',
                                             'domain': 'd',
                                             'type': 'floto.specs.task.ActivityTask',
                                             'version': '1'}]}}
        t = (decider.get_activity_tasks_from_input(d))
        assert isinstance(t[0], floto.specs.task.ActivityTask)
        assert t[0].name == 'fileLength'
                

