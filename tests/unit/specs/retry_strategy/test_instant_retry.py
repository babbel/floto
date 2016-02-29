import pytest
import floto.specs.retry_strategy

class TestInstantRetry(object):
    def test_is_task_resubmitted(self):
        s = floto.specs.retry_strategy.InstantRetry(retries=2)
        assert s.is_task_resubmitted(failures=2)
        assert not s.is_task_resubmitted(failures=3)
