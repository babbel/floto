import pytest
import floto.specs.retry_strategy

class TestInstantRetry(object):
    def test_is_task_resubmitted(self):
        s = floto.specs.retry_strategy.InstantRetry(retries=2)
        assert s.is_task_resubmitted(failures=2)
        assert not s.is_task_resubmitted(failures=3)

    def test_deserialized(self):
        j = floto.specs.retry_strategy.InstantRetry(retries=3).serializable()
        s = floto.specs.retry_strategy.InstantRetry.deserialized(**j)
        assert isinstance(s, floto.specs.retry_strategy.InstantRetry)
        assert s.retries == 3

    def test_deserialized_with_mother_class(self):
        j = floto.specs.retry_strategy.InstantRetry(retries=3).serializable()
        s = floto.specs.retry_strategy.Strategy.deserialized(**j)
        assert isinstance(s, floto.specs.retry_strategy.InstantRetry)
        assert s.retries == 3

