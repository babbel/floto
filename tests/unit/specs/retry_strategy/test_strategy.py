import pytest

import floto.specs.retry_strategy

class TestRetry(object):
    def test_init(self):
        s = floto.specs.retry_strategy.Strategy()
        with pytest.raises(NotImplementedError):
            s.is_task_resubmitted(failures=3)

    def test_serializable(self):
        j = floto.specs.retry_strategy.Strategy().serializable()
        assert j['type'] == 'floto.specs.retry_strategy.Strategy'

    def test_deserialized(self):
        s = floto.specs.retry_strategy.Strategy.deserialized()
        assert isinstance(s, floto.specs.retry_strategy.Strategy)





