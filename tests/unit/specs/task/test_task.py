import pytest
import floto.specs.task

class TestTask(object):
    def test_init(self):
        task = floto.specs.task.Task(id_='id', requires='r')
        assert task.id_ == 'id'
        assert task.requires == 'r'
    
    def test_default_id_wo_input(self):
        t = floto.specs.task.Task()
        assert t._default_id('n', 'v', None) == 'n:v:2be88ca4242c76e'

    def test_default_id_w_input(self):
        t = floto.specs.task.Task()
        assert t._default_id('n', 'v', {'foo':'bar'}) == 'n:v:bc4919c6adf7168'
