import pytest
from floto.specs import DeciderSpec
from floto.specs.task import ActivityTask
import floto.decider
from unittest.mock import Mock


@pytest.fixture
def task_1():
    return ActivityTask(name='activity1', version='v1', input={'date':1})

@pytest.fixture
def task_2(task_1):
    return ActivityTask(name='activity2', version='v1', requires=[task_1]) 

@pytest.fixture
def decider_spec(task_1, task_2):
    activity_tasks = [task_1, task_2]
    decider_spec = DeciderSpec(domain='d', task_list='tl', activity_tasks=activity_tasks,
            terminate_decider_after_completion=True)
    return decider_spec 

@pytest.fixture
def decider_spec_json(decider_spec):
    return decider_spec.to_json()

@pytest.fixture
def decider(decider_spec):
    return floto.decider.Decider(decider_spec=decider_spec)

class TestDecider():
    def test_init_with_decider_spec(self, decider_spec, task_1):
        d =  floto.decider.Decider(decider_spec=decider_spec)
        assert isinstance(d.decider_spec.activity_tasks, list)
        assert d.decider_spec.activity_tasks[0] == task_1 
        assert d.repeat_workflow == False
    
    def test_init_raises_wo_domain(self, decider_spec):
        decider_spec.domain = None
        with pytest.raises(ValueError):
            d =  floto.decider.Decider(decider_spec=decider_spec)

    def test_init_raises_wo_task_list(self, decider_spec):
        decider_spec.task_list = None
        with pytest.raises(ValueError):
            d =  floto.decider.Decider(decider_spec=decider_spec)

    def test_default_activity_task_list(self, decider_spec):
        d =  floto.decider.Decider(decider_spec=decider_spec)
        assert d.activity_task_list == 'floto_activities'

    def test_activity_task_list(self, decider_spec):
        decider_spec.activity_task_list = 'atl'
        d =  floto.decider.Decider(decider_spec=decider_spec)
        assert d.activity_task_list == 'atl'

    def test_domain(self, decider_spec):
        decider_spec.domain = 'my_domain'
        d =  floto.decider.Decider(decider_spec=decider_spec)
        assert d.domain == 'my_domain'

    def test_task_list(self, decider_spec):
        decider_spec.task_list = 'my_tl'
        d =  floto.decider.Decider(decider_spec=decider_spec)
        assert d.task_list == 'my_tl'

    def test_init_with_decider_spec_json(self, decider_spec_json):
        d =  floto.decider.Decider(decider_spec=decider_spec_json)
        assert isinstance(d.decider_spec, floto.specs.DeciderSpec)
        assert d.decider_spec.activity_tasks[0].input == {'date':1}

    def test_execution_graph(self, decider_spec_json):
        d =  floto.decider.Decider(decider_spec=decider_spec_json)
        assert isinstance(d.decision_builder.execution_graph, floto.decider.ExecutionGraph)

    def test_get_decisions(self, decider, mocker):
        mocker.patch('floto.decider.DecisionBuilder.get_decisions', return_value=['d'])
        mocker.patch('floto.decider.DecisionBuilder.is_terminate_workflow', return_value=True)
        decider.get_decisions()
        assert decider.decisions == ['d']
        assert decider.terminate_workflow == True

    def test_tear_down_do_not_repeat(self, decider):
        decider.swf.start_workflow_execution = Mock()
        decider.terminate_decider = False
        decider.tear_down()
        assert decider.terminate_decider == True
        assert not decider.swf.start_workflow_execution.called

    def test_tear_down_repeat(self, mocker, decider):
        info = {'executionInfo':{'workflowType':{'name':'wf_name', 'version':'v1'}}}
        mocker.patch('floto.decider.Base.get_workflow_execution_description', return_value=info)
        mocker.patch('floto.api.Swf.start_workflow_execution')
        mocker.patch('floto.decider.DecisionInput.get_input_workflow', return_value='wf_input')

        decider.repeat_workflow = True
        decider.domain = 'd'
        decider.task_list = 'tl'
        decider.tear_down()

        expected_args = {'domain':'d',
                         'workflow_type_name':'wf_name',
                         'workflow_type_version':'v1',
                         'task_list':'tl',
                         'input':'wf_input'}
        decider.swf.start_workflow_execution.assert_called_once_with(**expected_args)

