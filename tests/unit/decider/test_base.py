import pytest
import json

import floto
import floto.decider

from unittest.mock import PropertyMock, Mock
from floto.decisions import ScheduleActivityTask, CompleteWorkflowExecution

class BaseDeciderMock(floto.decider.Base):
    def __init__(self, swf=None):
        super().__init__(swf)
        self.max_polls = 1
        self.number_polls = 0

    def poll_for_decision(self):
        if self.max_polls < self.number_polls:
            super().poll_for_decision()
        else:
            self.terminate_decider = True
        self.number_polls += 1


class TestBase:
    def test_init(self):
        d = floto.decider.Base(swf='swf')
        assert d.swf == 'swf'

    def test_complete(self, mocker):
        client_mock = type("ClientMock", (object,), {'respond_decision_task_completed':Mock()})
        mocker.patch('floto.api.Swf.client', new_callable=PropertyMock, return_value=client_mock())
        mocker.patch('floto.decisions.ScheduleActivityTask.get_decision', return_value='my_dec')

        d = floto.decider.Base()
        d.decisions.append(ScheduleActivityTask())

        d.complete()
        ScheduleActivityTask.get_decision.assert_called_once_with()

        args = {'decisions': ['my_dec'], 'taskToken': d.task_token}
        floto.api.Swf.client.respond_decision_task_completed.assert_called_once_with(**args)
        assert d.decisions == []

    def test_complete_timed_out(self, mocker):
        raises = Mock(side_effect=Exception)
        client_mock = type("ClientMock", (object,), {'respond_decision_task_completed':raises})
        mocker.patch('floto.api.Swf.client', new_callable=PropertyMock, return_value=client_mock())

        d = floto.decider.Base()
        d.tear_down = Mock()
        d.terminate_workflow = True
        d.complete()
        assert d.terminate_workflow == False
        assert not d.tear_down.called

    def test_complete_tear_down(self, mocker): 
        client_mock = type("ClientMock", (object,), {'respond_decision_task_completed':Mock()})
        mocker.patch('floto.api.Swf.client', new_callable=PropertyMock, return_value=client_mock())

        d = floto.decider.Base()
        d.terminate_workflow = True
        d.complete()
        assert d.decisions == []

    def test_poll_for_decisions(self, mocker, init_response):
        mocker.patch('floto.api.Swf.poll_for_decision_task_page', return_value=init_response)
        d = floto.decider.Base(identity='did')
        d.domain = 'd'
        d.task_list = 't'
        d.poll_for_decision()
        assert d.history
        assert d.task_token == 'val_task_token'
        assert d.run_id == 'val_run_id'
        assert d.workflow_id == 'val_workflow_id'
        d.swf.poll_for_decision_task_page.assert_called_once_with(domain='d', task_list='t', 
                identity='did')

    def test_poll_for_decisions_with_empty_response(self, mocker):
        mocker.patch('floto.api.Swf.poll_for_decision_task_page', return_value={})
        d = floto.decider.Base()
        d.task_token = 'abc'
        d.history = 'h'

        d.poll_for_decision()
        assert d.history == None
        assert not d.task_token

    def test_get_decisions_raises(self):
        d = floto.decider.Base()
        with pytest.raises(NotImplementedError):
            d.get_decisions()

    def test_run(self, mocker):
        mocker.patch('floto.decider.Base.poll_for_decision')
        mocker.patch('floto.decider.Base.get_decisions')
        mocker.patch('floto.decider.Base.complete')
        d = BaseDeciderMock() 
        d.task_token = 'abc'
        d.run()
        d.get_decisions.assert_called_once_with()
        d.complete.assert_called_once_with()

    def test_run_in_separate_process(self, mocker):
        mocker.patch('floto.decider.Base.poll_for_decision')
        mocker.patch('floto.decider.Base.get_decisions')
        mocker.patch('floto.decider.Base.complete')
        d = BaseDeciderMock() 
        d.task_token = 'abc'
        d.run(separate_process=True)
        d._separate_process.join()
        assert True
        
    def test_get_workflow_execution_description(self, mocker):
        description = {'openCounts': {'openActivityTasks': 3}}
        mocker.patch('floto.api.Swf.describe_workflow_execution', return_value=description)
        d = floto.decider.Base()
        d.workflow_id = 'wid'
        d.run_id = 'rid'
        d.domain = 'd'
        assert d.get_workflow_execution_description()['openCounts']['openActivityTasks'] == 3
        d.swf.describe_workflow_execution.assert_called_once_with('d', 'wid', 'rid')


        


