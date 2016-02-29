import pytest

import floto.specs.retry_strategy

class TestRetry(object):
    def test_init(self):
        s = floto.specs.retry_strategy.Strategy()
        with pytest.raises(NotImplementedError):
            s.is_task_resubmitted(failures=3)


