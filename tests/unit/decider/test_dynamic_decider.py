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

@pytest.fixture
def decider(decider_spec):
    return floto.decider.DynamicDecider(decider_spec, identity='d_id')

class TestDynamicDecider:
    def test_init(self, decider, decider_spec):
        assert decider.decider_spec == decider_spec
        assert decider.identity == 'd_id'

