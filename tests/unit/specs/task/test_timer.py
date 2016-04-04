import floto.specs.task


class TestTimer:
    def test_init(self):
        t = floto.specs.task.Timer(id_='my_timer', requires=['t2'], delay_in_seconds=600)
        assert t.id_ == 'my_timer'
        assert t.requires == ['t2']
        assert t.delay_in_seconds == 600

    def test_serializable(self):
        t = floto.specs.task.Timer(id_='my_timer', delay_in_seconds=600)
        d = t.serializable()
        assert d['id_'] == 'my_timer'

        t2 = floto.specs.task.Timer(id_='my_timer2', requires=[t.id_], delay_in_seconds=600)
        d = t2.serializable()
        assert d['id_'] == 'my_timer2'
        assert d['requires'][0] == 'my_timer'

    def test_deserialized(self):
        kwargs = {'id_': 'my_timer', 
                  'delay_in_seconds': 600, 
                  'type': 'floto.specs.task.Timer', 
                  'requires': ['my_required_timer']}
        timer = floto.specs.task.Timer.deserialized(**kwargs)
        assert isinstance(timer, floto.specs.task.Timer)
        assert timer.delay_in_seconds == 600
