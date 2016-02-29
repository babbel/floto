import pytest
import floto.specs

class TestTask(object):
    def test_init(self):
        task = floto.specs.Task(id_='id', requires='r')
        assert task.id_ == 'id'
        assert task.requires == 'r'

