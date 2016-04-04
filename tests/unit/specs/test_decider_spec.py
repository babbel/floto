import pytest
import json

import floto.specs

@pytest.fixture
def task():
    return floto.specs.task.ActivityTask(domain='domain', name='at', version='2')

@pytest.fixture
def decider_spec(task):
        return floto.specs.DeciderSpec(domain='d',
                                       task_list='tl',
                                       activity_tasks=[task],
                                       default_activity_task_list='atl',
                                       repeat_workflow=True,
                                       terminate_decider_after_completion=True)


class TestDeciderSpec:
    def test_init(self, decider_spec, task):
        assert decider_spec.domain == 'd'
        assert decider_spec.task_list == 'tl'
        assert decider_spec.activity_tasks == [task]
        assert decider_spec.default_activity_task_list == 'atl'
        assert decider_spec.repeat_workflow is True
        assert decider_spec.terminate_decider_after_completion is True

    def test_serializable(self, decider_spec):
        s = decider_spec.serializable()
        expected = {'type': 'floto.specs.DeciderSpec', 
                    'domain': 'd', 
                    'task_list': 'tl', 
                    'default_activity_task_list': 'atl', 
                    'repeat_workflow': True, 
                    'terminate_decider_after_completion': True}
        assert isinstance(s, dict)
        assert isinstance(s['activity_tasks'], list)
        assert isinstance(s['activity_tasks'][0], dict)
        for k,v in s.items():
            assert s[k] == v

    def test_deserialized(self, decider_spec):
        activity_task = {'type': 'floto.specs.task.ActivityTask', 
                         'name': 'n', 
                         'version': '1', 
                         'domain': 'd'}

        kwargs = {'type': 'floto.specs.DeciderSpec', 
                  'domain': 'd', 
                  'task_list': 'tl', 
                  'default_activity_task_list': 'atl', 
                  'repeat_workflow': True, 
                  'terminate_decider_after_completion': True, 
                  'activity_tasks':[activity_task]}
        
        d = floto.specs.DeciderSpec.deserialized(**kwargs)
        assert isinstance(d, floto.specs.DeciderSpec)
        assert d.domain == 'd'
        assert d.task_list == 'tl'
        assert d.default_activity_task_list == 'atl'
        assert d.repeat_workflow == True
        assert d.terminate_decider_after_completion == True
        assert isinstance(d.activity_tasks[0], floto.specs.task.ActivityTask)

    def test_to_json(self, decider_spec, mocker):
        mocker.patch('floto.specs.DeciderSpec.serializable', return_value='serializable')
        j = decider_spec.to_json()
        assert j == json.dumps('serializable')

    def test_from_json(self, mocker):
        mocker.patch('floto.specs.DeciderSpec.deserialized')
        j = json.dumps({'type':'DeciderSpec'})
        floto.specs.DeciderSpec.from_json(j)
        floto.specs.DeciderSpec.deserialized.assert_called_once_with(type='DeciderSpec')

