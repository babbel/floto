import pytest
import floto.specs.task

@pytest.fixture
def child_workflow():
    retry_strategy = floto.specs.retry_strategy.Strategy()
    cw = floto.specs.task.ChildWorkflow(workflow_type_name='wft_name',
                                        workflow_type_version='wft_version',
                                        id_='wid',
                                        domain='d',
                                        requires=['r_id'],
                                        input={'foo': 'bar'},
                                        retry_strategy=retry_strategy)
    return cw

@pytest.fixture
def serialized_child_workflow():
    return {'domain': 'd', 
            'input': {'foo': 'bar'}, 
            'id_': 'wid', 
            'requires': ['r_id'], 
            'workflow_type_version': 'wft_version', 
            'retry_strategy': {'type': 'floto.specs.retry_strategy.Strategy'}, 
            'workflow_type_name': 'wft_name', 
            'type': 'floto.specs.task.ChildWorkflow'}

class TestChildWorkflow:
    def test_init(self, child_workflow):
        assert child_workflow.workflow_type_name == 'wft_name'
        assert child_workflow.workflow_type_version == 'wft_version'
        assert child_workflow.id_ == 'wid'
        assert child_workflow.requires == ['r_id']
        assert child_workflow.input == {'foo': 'bar'}
        assert child_workflow.retry_strategy

    def test_default_worfklow_id(self, mocker):
        mocker.patch('floto.specs.task.Task._default_id', return_value='did')
        cw = floto.specs.task.ChildWorkflow(workflow_type_name='wft_name',
                                            workflow_type_version='wft_version', domain='d',
                                            input={'foo': 'bar'})
        floto.specs.task.Task._default_id.assert_called_once_with(name='wft_name',
                                                                  version='wft_version',
                                                                  domain='d',
                                                                  input={'foo': 'bar'})
        assert cw.id_ == 'did'

    def test_deserialized(self, serialized_child_workflow):
        cw = floto.specs.task.ChildWorkflow.deserialized(**serialized_child_workflow)
        assert isinstance(cw, floto.specs.task.ChildWorkflow)
        assert isinstance(cw.retry_strategy, floto.specs.retry_strategy.Strategy)
        assert cw.id_ == 'wid'
        assert cw.requires == ['r_id']

    def test_serializable(self, child_workflow, serialized_child_workflow):
        serializable = child_workflow.serializable()
        assert serializable == serialized_child_workflow


