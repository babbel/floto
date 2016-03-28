import json

import floto.specs


class TestDeciderSpec:
    def test_init(self):
        d = floto.specs.DeciderSpec(domain='d',
                                    task_list='tl',
                                    activity_tasks=['t1'],
                                    default_activity_task_list='atl',
                                    repeat_workflow=True)
        assert d.domain == 'd'
        assert d.task_list == 'tl'
        assert d.activity_tasks == ['t1']
        assert d.default_activity_task_list == 'atl'
        assert d.repeat_workflow is True

    def test_to_json(self):
        d = floto.specs.DeciderSpec(domain='d', task_list='tl', activity_tasks=['t1'])
        j = d.to_json()
        assert j == json.dumps({'type': 'floto.specs.DeciderSpec',
                                'domain': 'd',
                                'activity_tasks': ['t1'],
                                'task_list': 'tl',
                                'repeat_workflow': False,
                                'terminate_decider_after_completion': False},
                               sort_keys=True)

    def test_to_json_default_activity_task_list(self):
        d = floto.specs.DeciderSpec(domain='d', activity_tasks=['t1'], task_list='tl',
                                    default_activity_task_list='atl')
        j = json.loads(d.to_json())
        assert j['default_activity_task_list'] == 'atl'

    def test_from_json(self):
        json_dict = {'type': 'floto.specs.DeciderSpec',
                     'domain': 'd',
                     'task_list': 'tl',
                     'default_activity_task_list': 'atl',
                     'repeat_workflow': True,
                     'activity_tasks': [{'type': 'floto.specs.task.ActivityTask',
                                         'name': 'name1',
                                         'id_': 'id1',
                                         'version': '1,',
                                         'domain': 'd',
                                         'retry_strategy': {
                                             'type': 'floto.specs.retry_strategy.InstantRetry',
                                             'retries': 3}},
                                        {'type': 'floto.specs.task.ActivityTask',
                                         'name': 'name2',
                                         'id_': 'id2',
                                         'version': '1,',
                                         'domain': 'd'}]
                     }
        json_str = json.dumps(json_dict)

        d = floto.specs.DeciderSpec.from_json(json_str)
        assert d
        assert d.task_list == 'tl'
        assert d.activity_tasks
        assert d.default_activity_task_list == 'atl'
        assert d.repeat_workflow is True
        assert set([t.name for t in d.activity_tasks]) == {'name1', 'name2'}
        assert set([t.id_ for t in d.activity_tasks]) == {'id1', 'id2'}
        assert d.activity_tasks[0].retry_strategy.is_task_resubmitted(2)
