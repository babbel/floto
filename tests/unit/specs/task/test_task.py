import pytest
import floto.specs.task

@pytest.fixture
def task():
    return floto.specs.task.Task()

class TestTask:
    def test_init(self):
        task = floto.specs.task.Task(id_='id', requires='r')
        assert task.id_ == 'id'
        assert task.requires == 'r'
    
    def test_default_id_format(self, task):
        assert 'n:v:' in task._default_id(domain='d', name='n', version='v', input=None)

    def test_default_id_same_input(self, task):
        input_1 = {'foo':'bar', 'foo2':'bar'}
        input_2 = {'foo2':'bar', 'foo':'bar'}
        id1 = task._default_id(domain='d', name='n', version='v', input=input_1) 
        id2 = task._default_id(domain='d', name='n', version='v', input=input_2)
        assert id1 == id2

    def test_default_id_different_input(self, task):
        id1 = task._default_id(domain='d', name='n', version='v', input='i1') 
        id2 = task._default_id(domain='d', name='n', version='v', input='i2')
        assert not id1 == id2

    def test_default_id_different_requires(self, task):
        t1 = floto.specs.task.Task(id_='t1')
        t2 = floto.specs.task.Task(id_='t2')

        task.requires = [t1]
        id_1 = task._default_id(domain='d', name='n', version='v', input='i')

        task.requires = [t2]
        id_2 = task._default_id(domain='d', name='n', version='v', input='i')
        assert not id_1 == id_2 

    def test_serializable(self, task):
        task.id_ = 'my_id'
        serializable = task.serializable()
        assert serializable['type'] == 'floto.specs.task.Task' 
        assert serializable['id_'] == 'my_id' 

    def test_serializable_with_requires(self):
        task_1 = floto.specs.task.Task(id_='task1')
        task_2 = floto.specs.task.Task(id_='task2', requires=[task_1])
        serializable = task_2.serializable()
        assert serializable['requires'] == [{'id_':'task1', 'type':'floto.specs.task.Task'}]


