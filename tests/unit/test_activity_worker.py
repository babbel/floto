import pytest
import json
from inspect import signature
from unittest.mock import PropertyMock, Mock

import floto.api

@pytest.fixture
def activity_task():
    return {'activityId': 'my_activity_id',
            'activityType': {'name': 'my_activity_type', 'version': 'v1'},
            'startedEventId': 7,
            'taskToken': 'the_task_token',
            'workflowExecution': {'runId': 'the_run_id', 'workflowId': 'the_workflow_id'}}

@pytest.fixture
def worker(mocker):
    mocked_functions = {'respond_activity_task_completed':Mock(),
                        'respond_activity_task_failed':Mock()}
    client_mock = type("ClientMock", (object,), mocked_functions)
    mocker.patch('floto.api.Swf.client', new_callable=PropertyMock, return_value=client_mock())
    worker = floto.ActivityWorker()
    worker.result = 'result'
    worker.task_token = 'tt'
    return worker

class TestActivityWorker:
    def test_init(self):
        worker = floto.ActivityWorker(task_heartbeat_in_seconds=10)
        assert worker.task_heartbeat_in_seconds == 10

    def test_init_with_defaults(self):
        worker = floto.ActivityWorker()
        assert worker.task_heartbeat_in_seconds == 120

    def test_terminate_activity_worker(self):
        worker = floto.ActivityWorker()
        worker.terminate_worker()
        assert worker._terminate_activity_worker == True

    def test_start_heartbeat(self, mocker):
        mocker.patch('floto.HeartbeatSender.send_heartbeats')
        worker = floto.ActivityWorker()
        worker.task_heartbeat_in_seconds = 1
        worker.task_token = 't'
        worker.start_heartbeat()
        worker.heartbeat_sender.send_heartbeats.assert_called_once_with(timeout=1, task_token='t')

    def test_stop_heartbeat(self, mocker):
        mocker.patch('floto.HeartbeatSender.stop_heartbeats')
        worker = floto.ActivityWorker()
        worker.stop_heartbeat()
        worker.heartbeat_sender.stop_heartbeats.assert_called_once_with()


@floto.activity(name='my_activity_type', version='v1')
def do_work():
    return 'success'


@floto.activity(name='my_activity_type', version='v2')
def do_work_and_return_context(context):
    return context


@floto.activity(name='activity_fails', version='v1')
def fail():
    raise ValueError('Activity failed')


class MockedActivityWorker(floto.ActivityWorker):
    def task_failed(self, error):
        self._error = error


class TestActivityWorkerAPICalls:
    def test_args(self):
        sig = signature(floto.ACTIVITY_FUNCTIONS['my_activity_type:v2'])
        assert ('context' in sig.parameters)

    def test_poll(self, mocker, activity_task):
        mocker.patch('floto.api.Swf.poll_for_activity_task', return_value=activity_task)

        worker = floto.ActivityWorker()
        worker.domain = 'd'
        worker.task_list = 'task_list'
        worker.identity = 'test machine'

        worker.poll()
        worker.swf.poll_for_activity_task.assert_called_once_with(task_list='task_list', domain='d',
                                                                  identity='test machine')

        assert worker.task_token == 'the_task_token'
        assert worker.last_response['activityId'] == 'my_activity_id'

    def test_run_with_response(self, mocker, activity_task):
        mocker.patch('floto.api.Swf.poll_for_activity_task', return_value=activity_task)
        mocker.patch('floto.ActivityWorker.complete')

        worker = floto.ActivityWorker()
        worker.max_polls = 1
        worker.run()

        worker.complete.assert_called_once_with()

    def test_run_with_context(self, mocker):
        activity_task_with_input = {'activityId': 'my_activity_id',
                                    'activityType': {'name': 'my_activity_type', 'version': 'v2'},
                                    'taskToken': 'the_task_token',
                                    'input': {'foo': 'bar'}}
        mocker.patch('floto.api.Swf.poll_for_activity_task', return_value=activity_task_with_input)
        mocker.patch('floto.ActivityWorker.complete')

        worker = floto.ActivityWorker()
        worker.max_polls = 1
        worker.run()
        assert worker.result == {'foo': 'bar'}

    def test_run_with_context_wo_input(self, mocker):
        activity_task_with_input = {'activityId': 'my_activity_id',
                                    'activityType': {'name': 'my_activity_type', 'version': 'v2'},
                                    'taskToken': 'the_task_token'}
        mocker.patch('floto.api.Swf.poll_for_activity_task', return_value=activity_task_with_input)
        mocker.patch('floto.ActivityWorker.complete')

        worker = floto.ActivityWorker()
        worker.max_polls = 1
        worker.run()
        assert worker.result == {}

    def test_run_without_response(self, mocker):
        mocker.patch('floto.api.Swf.poll_for_activity_task', return_value={})
        mocker.patch('floto.ActivityWorker.complete')

        worker = floto.ActivityWorker()
        worker.max_polls = 1
        worker.run()

        worker.complete.assert_not_called()

    def test_run_with_failing_activity(self, mocker):
        failing_task = {'activityType': {'name': 'activity_fails', 'version': 'v1'},
                        'taskToken': 'the_task_token'}
        mocker.patch('floto.api.Swf.poll_for_activity_task', return_value=failing_task)
        mocker.patch('floto.ActivityWorker.complete')

        worker = MockedActivityWorker()
        worker.max_polls = 1
        worker.run()
        assert str(worker._error) == 'Activity failed'

    def test_run_with_activity_not_found(self, mocker):
        failing_task = {'activityType': {'name': 'activity_unknown', 'version': 'v1'},
                        'taskToken': 'the_task_token'}
        mocker.patch('floto.api.Swf.poll_for_activity_task', return_value=failing_task)
        mocker.patch('floto.ActivityWorker.complete')

        worker = MockedActivityWorker()
        worker.max_polls = 1
        worker.run()
        message = 'No activity with id activity_unknown:v1 registered'
        assert str(worker._error) == message

    def test_task_failed(self, mocker):
        client_mock = type('ClientMock', (object,), {'respond_activity_task_failed': Mock()})
        mocker.patch('floto.api.Swf.client', new_callable=PropertyMock, return_value=client_mock())

        worker = floto.ActivityWorker()
        worker.task_token = 'abc'
        worker.task_failed(Exception('some error'))
        expected_args = {'taskToken': 'abc', 'details': 'some error'}
        worker.swf.client.respond_activity_task_failed.assert_called_once_with(**expected_args)

    def test_terminate_worker(self):
        worker = floto.ActivityWorker()
        worker.terminate_worker()
        assert worker.get_terminate_activity_worker()

    def test_complete_result_string(self, mocker, worker):
        worker.result = 'result'
        worker.complete()
        assertion = {'taskToken':worker.task_token, 'result':'result'}
        worker.swf.client.respond_activity_task_completed.assert_called_once_with(**assertion)

    def test_complete_result_object(self, mocker, worker):
        worker.result = {'foo':'bar'}
        worker.complete()
        assertion = {'taskToken':worker.task_token, 'result':json.dumps({'foo':'bar'})}
        worker.swf.client.respond_activity_task_completed.assert_called_once_with(**assertion)

    def test_run_start_stop_heartbeats_with_successful_activity(self, mocker, activity_task):
        mocker.patch('floto.ActivityWorker.complete')
        mocker.patch('floto.ActivityWorker.start_heartbeat')
        mocker.patch('floto.ActivityWorker.stop_heartbeat')
        mocker.patch('floto.api.Swf.poll_for_activity_task', return_value=activity_task)
        worker = floto.ActivityWorker()
        worker.max_polls = 1
        worker.run()
        worker.start_heartbeat.assert_called_once_with()
        worker.stop_heartbeat.assert_called_once_with()

    def test_run_start_stop_heartbeats_with_failing_activity(self, mocker):
        failing_task = {'activityType': {'name': 'activity_fails', 'version': 'v1'},
                        'taskToken': 'the_task_token'}
        mocker.patch('floto.ActivityWorker.task_failed')
        mocker.patch('floto.ActivityWorker.start_heartbeat')
        mocker.patch('floto.ActivityWorker.stop_heartbeat')
        mocker.patch('floto.api.Swf.poll_for_activity_task', return_value=failing_task)
        worker = floto.ActivityWorker()
        worker.max_polls = 1
        worker.run()
        worker.start_heartbeat.assert_called_once_with()
        worker.stop_heartbeat.assert_called_once_with()

    def test_run_start_stop_heartbeats_with_non_existing_activity(self, mocker):
        failing_task = {'activityType': {'name': 'activity_unknown', 'version': 'v1'},
                        'taskToken': 'the_task_token'}
        mocker.patch('floto.ActivityWorker.task_failed')
        mocker.patch('floto.ActivityWorker.start_heartbeat')
        mocker.patch('floto.ActivityWorker.stop_heartbeat')
        mocker.patch('floto.api.Swf.poll_for_activity_task', return_value=failing_task)
        worker = floto.ActivityWorker()
        worker.max_polls = 1
        worker.run()
        worker.start_heartbeat.assert_not_called()

    def test_start_heartbeat_deactivation(self):
        worker = floto.ActivityWorker(task_heartbeat_in_seconds=0)
        worker.heartbeat_sender.send_heartbeats = Mock()
        worker.start_heartbeat()
        assert not worker.heartbeat_sender.send_heartbeats.called

