import pytest
import floto.decider

@pytest.fixture
def history(init_response):
    return floto.History(domain='d', task_list='tl', response=init_response)

@pytest.fixture
def daemon(history):
    daemon = floto.decider.Daemon(domain='d', task_list='tl_daemon', swf='swf')
    daemon.history = history
    return daemon

@pytest.fixture
def decider_spec():
    task1 = floto.specs.task.ActivityTask(domain='d',name='at', version='v1')
    spec = floto.specs.DeciderSpec(domain='d', task_list='tl', activity_tasks=[task1])
    return spec

@pytest.fixture
def json_decider_spec(decider_spec):
    return decider_spec.to_json()

class TestDaemon():
    def test_init(self):
        assert floto.decider.Daemon().task_list == 'floto_daemon'

    def test_init_with_swf(self):
        d = floto.decider.Daemon(swf='my_swf')
        assert d.swf == 'my_swf'

    def test_init_with_args(self):
        d = floto.decider.Daemon(domain='d', task_list='tl')
        assert d.task_list == 'tl'
        assert d.domain == 'd'

    def test_get_decisions_child_workflow(self, mocker, daemon):
        mocker.patch('floto.decider.Daemon.get_decision_child_workflow', return_value='d')
        decisions = daemon.get_decisions_child_workflows(['cwf1'])
        daemon.get_decision_child_workflow.assert_called_once_with('cwf1')
        assert decisions == ['d']

    def test_get_decision_child_workflow(self, daemon, json_decider_spec, mocker):
        signal_event = {'eventType':'WorkflowExecutionSignaled',
                        'workflowExecutionSignaledEventAttributes':{
                            'signalName':'startChildWorkflowExecution',
                            'input':{'decider_spec':json_decider_spec}}}

        decision = daemon.get_decision_start_child_workflow_execution()
        mocker.patch('floto.decider.Daemon.get_decision_start_child_workflow_execution',
            return_value=decision)
        mocker.patch('floto.decider.Daemon.start_child_decider')
        mocker.patch('floto.decider.Daemon.get_decider_spec')
        decision = daemon.get_decision_child_workflow(signal_event)
        daemon.get_decider_spec.assert_called_once_with(json_decider_spec,
                                                        decision.task_list['name'], 
                                                        daemon.domain)

    def test_get_decider_spec(self, json_decider_spec, daemon):
        spec = daemon.get_decider_spec(json_decider_spec, 'tl', 'd')
        assert isinstance(spec, floto.specs.DeciderSpec)
        assert spec.task_list == 'tl'
        assert spec.domain == 'd'
        assert spec.activity_tasks[0].name == 'at'




