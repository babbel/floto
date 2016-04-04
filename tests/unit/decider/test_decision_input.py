import pytest
import floto.decider
import floto.specs.task


@pytest.fixture
def task_1():
    return floto.specs.task.ActivityTask(domain='d', name='t', version='v1', input={'task_1':'val'})

@pytest.fixture
def task_2():
    return floto.specs.task.ActivityTask(domain='d', name='t2', version='v1', input={'task_2':'val'})

@pytest.fixture
def history(init_response):
    return floto.History(domain='d', task_list='tl', response=init_response)

@pytest.fixture
def di(history):
    di = floto.decider.DecisionInput()
    di.history = history 
    return di

@pytest.fixture
def child_workflow():
    return floto.specs.task.ChildWorkflow(domain='d', workflow_type_name='cw', workflow_type_version='v1')

class TestDecisionInput:
    def test_get_input(self, di, task_1):
        i = di.get_input(task_1, 'activity_task', None)
        assert i['activity_task'] == {'task_1':'val'}

    def test_get_input_task_with_dependencies(self, di, task_1, task_2, mocker):
        mocker.patch('floto.History.get_result_completed_activity', return_value='result_task_2')
        i = di.get_input(task_1, 'activity_task', [task_2])
        assert i['activity_task'] == {'task_1':'val'}
        assert i[task_2.id_] == 'result_task_2'

    def test_get_input_w_wf_input(self, di, task_1):
        i = di.get_input(task_1, 'activity_task', None)
        assert i['activity_task'] == {'task_1':'val'}
        assert i['workflow'] == 'workflow_input' 

    def test_get_input_wo_generator_result(self, di, mocker, task_1):
        mocker.patch('floto.History.get_result_completed_activity')
        g = floto.specs.task.Generator(domain='d', name='g', version='1')
        di.get_input(task_1, 'activity_task', [g])
        di.history.get_result_completed_activity.assert_not_called()

    def test_get_details_failed_tasks(self, mocker, di):
        mocker.patch('floto.History.get_id_activity_task_event', return_value='a_id')
        task_failed_event = {'eventType':'ActivityTaskFailed',
                             'activityTaskFailedEventAttributes':{'details':'Error'}}
        d = di.get_details_failed_tasks([task_failed_event])
        assert d['a_id'] == 'Error'

