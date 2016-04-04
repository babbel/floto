import pytest
import floto.specs.task

@pytest.fixture
def required_task():
    return floto.specs.task.ActivityTask(domain='d', name='r', version='2', id_='r_id',
                                         task_list='tl')
@pytest.fixture
def task(required_task):
    rs = floto.specs.retry_strategy.InstantRetry(retries=3)
    return floto.specs.task.ActivityTask(domain='d', name='n', version='1', id_='a_id',
                                         input={'foo':'bar'}, retry_strategy=rs, task_list='tl',
                                         requires=[required_task.id_])

@pytest.fixture
def serialized_task():
   return {'domain': 'd', 
           'input': {'foo': 'bar'}, 
           'type': 'floto.specs.task.ActivityTask', 
           'requires': ['r_id'], 
           'retry_strategy': {'type': 'floto.specs.retry_strategy.InstantRetry', 'retries': 3}, 
           'id_': 'a_id', 
           'task_list': 'tl', 
           'name': 'n', 
           'version': '1'}

class TestActivityTask:
    def test_init(self, task):
        assert task.name == 'n'
        assert task.version == '1'
        assert task.id_ == 'a_id'
        assert [id_ for id_ in task.requires] == ['r_id']
        assert task.input == {'foo':'bar'}
        assert task.retry_strategy.retries == 3
        assert task.task_list == 'tl'

    def test_init_wo_activity_id(self):
        t = floto.specs.task.ActivityTask(domain='d', name='n', version='v')
        assert t.id_ == t._default_id(domain='d', name='n', version='v', input=None)

    def test_serializable(self, task, serialized_task):
        s = task.serializable()
        assert s == serialized_task

    def test_serializable_with_retry_strategy(self, task):
        s = task.serializable()
        assert s['retry_strategy']['retries'] == 3

    def test_deserialized(self, serialized_task):
        task = floto.specs.task.ActivityTask.deserialized(**serialized_task)
        assert isinstance(task, floto.specs.task.ActivityTask)
        assert task.id_ == 'a_id'

