import pytest
import floto.decisions

class TestStartTimer(object):
    def test_init(self):
        t = floto.decisions.StartTimer(timer_id='id', start_to_fire_timeout=600)
        d = t._get_decision()
        attributes = d['startTimerDecisionAttributes']
        assert d['decisionType'] == 'StartTimer'
        assert attributes['startToFireTimeout'] == '600'
        assert attributes['timerId'] == 'id'
