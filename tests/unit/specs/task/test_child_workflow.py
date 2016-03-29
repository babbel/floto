import floto.specs.task


class TestChildWorkflow:
    def test_init(self):
        task = floto.specs.task.Task(id_='task_id')
        cw = floto.specs.task.ChildWorkflow(workflow_type_name='wft_name',
                                            workflow_type_version='wft_version',
                                            workflow_id='wid',
                                            domain='d',
                                            requires=[task],
                                            input={'foo': 'bar'},
                                            retry_strategy='retry_strategy')

        assert cw.workflow_type_name == 'wft_name'
        assert cw.workflow_type_version == 'wft_version'
        assert cw.id_ == 'wid'
        assert cw.requires == [task]
        assert cw.input == {'foo': 'bar'}
        assert cw.retry_strategy == 'retry_strategy'

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
