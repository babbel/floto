import pytest
import floto.specs.task

@pytest.fixture
def required_task():
    return floto.specs.task.ActivityTask(domain='d', name='r', version='2', activity_id='r_id',
                                         task_list='tl')
@pytest.fixture
def task(required_task):
    rs = floto.specs.retry_strategy.InstantRetry(retries=3)
    return floto.specs.task.ActivityTask(domain='d', name='n', version='1', activity_id='a_id',
                                         input={'foo':'bar'}, retry_strategy=rs, task_list='tl',
                                         requires=[required_task])

class TestActivityTask:
    def test_init(self, task):
        assert task.name == 'n'
        assert task.version == '1'
        assert task.id_ == 'a_id'
        assert [t.id_ for t in task.requires] == ['r_id']
        assert task.input == {'foo':'bar'}
        assert task.retry_strategy.retries == 3
        assert task.task_list == 'tl'

    def test_init_wo_activity_id(self):
        t = floto.specs.task.ActivityTask(domain='d', name='n', version='v')
        assert t.id_ == t._default_id(domain='d', name='n', version='v', input=None)

    def test_serializable_with_retry_strategy(self, task):
        s = task.serializable()
        assert s['retry_strategy']['retries'] == 3

    def test_deserialize(self):
        s = task.serializable()


