import pytest
import floto.specs.task

class TestActivityTask(object):
    def test_init(self):
        t = floto.specs.task.ActivityTask(name='n', version='v', activity_id='a_id', 
                requires=['t2'], input={'foo':'bar'}, retry_strategy='rs')
        assert t.name == 'n'
        assert t.version == 'v'
        assert t.id_ == 'a_id'
        assert t.requires == ['t2']
        assert t.input == {'foo':'bar'}
        assert t.retry_strategy == 'rs'

    def test_init_wo_activity_id(self):
        t = floto.specs.task.ActivityTask(name='n', version='v')
        assert t.id_ == t._default_id('n', 'v', None)
