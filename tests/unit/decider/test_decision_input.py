import pytest
import floto.decider


@pytest.fixture
def task_1():
    return floto.specs.ActivityTask(name='t', version='v1', input={'task_1':'val'})

@pytest.fixture
def task_2():
    return floto.specs.ActivityTask(name='t2', version='v1', input={'task_2':'val'})

@pytest.fixture
def history(init_response):
    return floto.History(domain='d', task_list='tl', response=init_response)

@pytest.fixture
def execution_graph(task_1):
    return floto.decider.ExecutionGraph(activity_tasks=[task_1])

@pytest.fixture
def di(history, execution_graph):
    di = floto.decider.DecisionInput(execution_graph=execution_graph)
    di._workflow_input = {'wf':'input'}
    di.history = history 
    return di

@pytest.fixture
def child_workflow():
    return floto.specs.ChildWorkflow(workflow_type_name='cw', workflow_type_version='v1')

class TestDecisionInput:
    def test_get_input_task_activity_task(self, di, task_1, mocker):
        mocker.patch('floto.decider.DecisionInput._get_input')
        di.get_input_task(task_1)
        di._get_input.assert_called_once_with(task_1, 'activity_task')

    def test_get_input_task_child_workflow(self, di, child_workflow, mocker):
        mocker.patch('floto.decider.DecisionInput._get_input')
        di.get_input_task(child_workflow)
        di._get_input.assert_called_once_with(child_workflow, 'child_workflow_task')

    def test_get_input_task_failed_task(self, di, task_1, mocker):
        mocker.patch('floto.decider.DecisionInput._get_input_scheduled_task')
        di.get_input_task(task_1, is_failed_task=True)
        di._get_input_scheduled_task.assert_called_once_with(task_1.id_, 'ActivityTaskScheduled')

    def test_get_input_task_failed_child_workflow(self, di, child_workflow, mocker):
        mocker.patch('floto.decider.DecisionInput._get_input_scheduled_task')
        di.get_input_task(child_workflow, is_failed_task=True)
        di._get_input_scheduled_task.assert_called_once_with(child_workflow.id_, 
                'StartChildWorkflowExecutionInitiated')

    def test_get_workflow_result_wo_result(self, di, mocker):
        mocker.patch('floto.History.get_result_completed_activity', return_value=None)
        assert di.get_workflow_result() == None

    def test_get_workflow_result(self, di, mocker, task_1):
        mocker.patch('floto.History.get_result_completed_activity', return_value={'foo':'bar'})
        assert di.get_workflow_result()[task_1.id_] == {'foo':'bar'}

    def test_get_input_task_unknow_type(self, di):
        assert not di.get_input_task('task')

    def test_get_input_workflow(self, di, mocker):
        mocker.patch('floto.History.get_workflow_input', return_value='wf_input')
        di._workflow_input = None
        i = di.get_input_workflow()
        assert i == 'wf_input'

    def test_store_input_workflow(self, di, mocker):
        di._workflow_input = 'wf_input_stored'
        i = di.get_input_workflow()
        assert i == 'wf_input_stored'

    def test_get_input(self, di, task_1):
        i = di._get_input(task_1, 'activity_task')
        assert i['activity_task'] == {'task_1':'val'}

    def test_get_input_task_with_dependencies(self, di, task_1, task_2, mocker):
        mocker.patch('floto.decider.ExecutionGraph.get_dependencies', return_value=[task_2])
        mocker.patch('floto.History.get_result_completed_activity', return_value='result_task_2')
        i = di._get_input(task_1, 'activity_task')
        assert i['activity_task'] == {'task_1':'val'}
        assert i[task_2.id_] == 'result_task_2'

    def test_get_input_w_wf_input(self, di, task_1):
        i = di._get_input(task_1, 'activity_task')
        assert i['activity_task'] == {'task_1':'val'}
        assert i['workflow'] == {'wf':'input'}

    def test_get_input_scheduled_task(self, di, mocker):
        scheduled_event = {'eventId':2, 
                   'eventType':'ActivityTaskScheduled', 
                   'activityTaskScheduledEventAttributes':{'input':'"my_input"'}}
        mocker.patch('floto.History.get_event_by_task_id_and_type', return_value=scheduled_event)
        i = di._get_input_scheduled_task('id', 'ActivityTaskScheduled')
        assert i == 'my_input' 

    def test_get_details_failed_tasks(self, mocker, di):
        mocker.patch('floto.History.get_id_activity_task_event', return_value='a_id')
        task_failed_event = {'eventType':'ActivityTaskFailed',
                             'activityTaskFailedEventAttributes':{'details':'Error'}}
        d = di.get_details_failed_tasks([task_failed_event])
        assert d['a_id'] == 'Error'

