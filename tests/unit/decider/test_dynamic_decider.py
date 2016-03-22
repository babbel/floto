import pytest
import floto.decider
from floto.specs.task import ActivityTask
from floto.specs import DeciderSpec 

@pytest.fixture
def task():
    return ActivityTask(name='activity2', version='v1') 

@pytest.fixture
def decider_spec(task):
    activity_tasks = [task]
    decider_spec = DeciderSpec(domain='d', task_list='tl', activity_tasks=activity_tasks,
            terminate_decider_after_completion=True)
    return decider_spec

# TODO test everything
class TestDynamicDecider:
    def test_init(self, decider_spec):
        d = floto.decider.DynamicDecider(decider_spec, identity='d_id')
        assert d.decider_spec == decider_spec
        assert d.identity == 'd_id'



