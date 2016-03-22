import pytest
import floto.specs.task

@pytest.fixture
def task():
    return floto.specs.task.Task()

class TestTask(object):
    def test_init(self):
        task = floto.specs.task.Task(id_='id', requires='r')
        assert task.id_ == 'id'
        assert task.requires == 'r'
    
    def test_default_id_format(self, task):
        assert 'n:v:' in task._default_id('n', 'v', None)

    def test_default_id_same_input(self, task):
        input_1 = {'foo':'bar', 'foo2':'bar'}
        input_2 = {'foo2':'bar', 'foo':'bar'}
        assert task._default_id('n', 'v', input_1) == task._default_id('n', 'v', input_2)

    def test_default_id_different_input(self, task):
        assert not task._default_id('n', 'v', 'i1') == task._default_id('n', 'v', 'i2')

    def test_default_id_different_requires(self, task):
        t1 = floto.specs.task.Task(id_='t1')
        t2 = floto.specs.task.Task(id_='t2')

        task.requires = [t1]
        id_1 = task._default_id('n', 'v', 'i')

        task.requires = [t2]
        id_2 = task._default_id('n', 'v', 'i')
        assert not id_1 == id_2 




