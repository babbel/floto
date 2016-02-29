import pytest
import floto.specs

class TestTimer(object):
    def test_init(self):
        t = floto.specs.Timer(id_='my_timer', requires=['t2'], delay_in_seconds=600)
        assert t.id_ == 'my_timer'
        assert t.requires == ['t2']
        assert t.delay_in_seconds == 600
