import floto.api

import pytest
import botocore.client
import botocore.exceptions
from unittest.mock import PropertyMock, Mock
import json

class Client_Mock(object):
    def register_workflow_type(self, **args):
        pass

class TestSwf(object):
    @classmethod
    def teardown_class(cls):
        print('tearing down class...')

    def test_init(self):
        swf = floto.api.Swf()
        assert type(swf.client).__name__ == 'SWF'
        assert isinstance(swf.domains, floto.api.Domains)
        assert isinstance(swf.domains.swf, floto.api.Swf)

    def test_client_params_from_init(self, mocker):
        mocker.patch('floto.api.Swf.open_session')
        client_params = { 'region_name':'region'}
        swf = floto.api.Swf(**client_params)
        swf.open_session.assert_called_once_with(client_params)

    def test_init_client_parameters(self, mocker):
        mocker.patch('floto.api.Swf.open_session')
        swf = floto.api.Swf(region_name='eu-west')
        client_params = {'region_name':'eu-west'}

        swf.open_session.assert_called_once_with(client_params)

    @pytest.mark.parametrize("args, api_args",[
        ({}, {}),
        ({'page_token':123}, {'nextPageToken':123}),
        ({'page_size':10}, {'maximumPageSize':10})])
    def test_poll_for_decision_task_page(self, mocker, args, api_args):
        client_mock = type("ClientMock", (object,), {"poll_for_decision_task":Mock()})
        mocker.patch('floto.api.Swf.client', new_callable=PropertyMock, return_value=client_mock())

        default_args = {'domain':'d', 'task_list':'tl'}
        default_args.update(args)

        default_api_args = {'domain':'d', 'taskList':{'name':'tl'}, 'reverseOrder':True, 
            'maximumPageSize':400}
        default_api_args.update(api_args)

        swf = floto.api.Swf()
        swf.poll_for_decision_task_page(**default_args)
        swf.client.poll_for_decision_task.assert_called_once_with(**default_api_args)

    def test_poll_for_decision_task_page_raises(self, mocker):
        swf = floto.api.Swf()
        with pytest.raises(ValueError):
            swf.poll_for_decision_task_page(domain='d')

    def test_register_activity_type(self, mocker):
        client_mock = type("ClientMock", (object,), {"register_activity_type":Mock()})
        mocker.patch('floto.api.Swf.client', new_callable=PropertyMock, return_value=client_mock())

        activity_type = floto.api.ActivityType()
        properties = activity_type._get_properties()

        swf = floto.api.Swf()
        swf.register_activity_type(activity_type)
        swf.client.register_activity_type.assert_called_once_with(**properties)

    def test_register_workflow_type(self, mocker):
        client_mock = type("ClientMock", (object,), {"register_workflow_type":Mock()})
        mocker.patch('floto.api.Swf.client', new_callable=PropertyMock, return_value=client_mock())

        w = floto.api.WorkflowType()
        args = w._get_properties()
        swf = floto.api.Swf()
        swf.register_workflow_type(w)
        swf.client.register_workflow_type.assert_called_once_with(**args)

    def test_register_type_does_not_raise(self, mocker):
        error_response = {'Error':{'Code':'TypeAlreadyExistsFault'}}
        client_error = botocore.exceptions.ClientError(error_response=error_response,
                operation_name="op_name")

        mock_function = Mock(side_effect=client_error)
        client_mock = type("ClientMock", (object,), {"register_workflow_type":mock_function})
        mocker.patch('floto.api.Swf.client', new_callable=PropertyMock, return_value=client_mock())

        swf = floto.api.Swf()

        workflow_type = floto.api.WorkflowType()
        swf.register_type(workflow_type)
        swf.client.register_workflow_type.assert_called_once_with(**workflow_type._get_properties())


    def test_start_workflow_execution(self, mocker):
        client_mock = type("ClientMock", (object,), {"start_workflow_execution":Mock()})
        mocker.patch('floto.api.Swf.client', new_callable=PropertyMock, return_value=client_mock())

        swf = floto.api.Swf()
        swf.start_workflow_execution(domain='test_domain', workflow_type_name='my_workflow_type',
                workflow_type_version='v1')

        args = {'domain':'test_domain',
                'workflowId':'my_workflow_type_v1',
                'workflowType':{'name':'my_workflow_type', 'version':'v1'}}
        swf.client.start_workflow_execution.assert_called_once_with(**args)

    def test_start_workflow_with_input(self, mocker):
        client_mock = type("ClientMock", (object,), {"start_workflow_execution":Mock()})
        mocker.patch('floto.api.Swf.client', new_callable=PropertyMock, return_value=client_mock())

        swf = floto.api.Swf()
        swf.start_workflow_execution(domain='test_domain', workflow_type_name='my_workflow_type',
                workflow_type_version='v1', input={'foo':'bar'})

        args = {'domain':'test_domain',
                'workflowId':'my_workflow_type_v1',
                'workflowType':{'name':'my_workflow_type', 'version':'v1'},
                'input':json.dumps({'foo':'bar'})}
        swf.client.start_workflow_execution.assert_called_once_with(**args)

    @pytest.mark.parametrize('args,expected',
            [({'domain':'d', 'workflow_id':'id', 'signal_name':'my_signal'},
              {'domain':'d', 'workflowId':'id', 'signalName':'my_signal'}),
              
              ({'domain':'d', 'workflow_id':'id', 'signal_name':'my_signal', 'input':'in'},
               {'domain':'d', 'workflowId':'id', 'signalName':'my_signal', 'input':'in'}),

              ({'domain':'d', 'workflow_id':'id', 'signal_name':'my_signal', 'input':{'f':'b'}},
              {'domain':'d', 'workflowId':'id', 'signalName':'my_signal', 'input':'{"f": "b"}'}),

              ({'domain':'d', 'workflow_id':'id', 'signal_name':'my_signal', 'run_id':'rid'},
              {'domain':'d', 'workflowId':'id', 'signalName':'my_signal', 'runId':'rid'})])
    def test_signal_workflow_execution(self, args, expected, mocker):
        client_mock = type("ClientMock", (object,), {"signal_workflow_execution":Mock()})
        mocker.patch('floto.api.Swf.client', new_callable=PropertyMock, return_value=client_mock())

        swf = floto.api.Swf()
        swf.signal_workflow_execution(**args)
        swf.client.signal_workflow_execution.assert_called_once_with(**expected)

    @pytest.mark.parametrize('args,expected',
            [({'domain':'d', 'workflow_id':'id'},
              {'domain':'d', 'workflowId':'id'}),
              
              ({'domain':'d', 'workflow_id':'id', 'run_id':'my_run_id'},
               {'domain':'d', 'workflowId':'id', 'runId':'my_run_id'})])
    def test_terminate_workflow_exeuction(self, args, expected, mocker):
        client_mock = type("ClientMock", (object,), {"terminate_workflow_execution":Mock()})
        mocker.patch('floto.api.Swf.client', new_callable=PropertyMock, return_value=client_mock())

        swf = floto.api.Swf()
        swf.terminate_workflow_execution(**args)
        swf.client.terminate_workflow_execution.assert_called_once_with(**expected)

    @pytest.mark.parametrize('args',[({'domain':'d'}), ({'workflow_id':'id'}), ({})])
    def test_termiante_workflow_execution_raises(self, args):
        with pytest.raises(ValueError):
            swf = floto.api.Swf()
            swf.terminate_workflow_execution(**args)

    @pytest.mark.parametrize('args', [
        ({}),
        ({'workflow_type_name':'name', 'workflow_type_version':'v1'}),
        ({'domain':'domain', 'workflow_type_version':'v1'}),
        ({'domain':'domain', 'workflow_type_name':'name'})])
    def test_start_workflow_raises(self, args):
        swf = floto.api.Swf()
        with pytest.raises(ValueError):
            swf.start_workflow_execution(**args)

    def test_describe_workflow_execution(self, mocker):
        client_mock = type("ClientMock", (object,), {'describe_workflow_execution':Mock()})
        mocker.patch('floto.api.Swf.client', new_callable=PropertyMock, return_value=client_mock())

        swf = floto.api.Swf()
        swf.describe_workflow_execution('d', 'wid', 'rid')
        expected = {'domain':'d',
                    'execution':{'workflowId': 'wid', 'runId': 'rid'}}
        swf.client.describe_workflow_execution.assert_called_once_with(**expected)

    def test_record_activity_heartbeat(self, mocker):
        client_mock = type("ClientMock", (object,), {'record_activity_task_heartbeat':Mock()})
        mocker.patch('floto.api.Swf.client', new_callable=PropertyMock, return_value=client_mock())

        swf = floto.api.Swf()
        swf.record_activity_task_heartbeat(task_token='token', details='my_details')
        expected = {'taskToken':'token', 'details':'my_details'}
        swf.client.record_activity_task_heartbeat.assert_called_once_with(**expected)
