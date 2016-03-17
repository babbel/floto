import floto
import logging
from floto.specs import ActivityTask, DeciderSpec, ChildWorkflow
import floto.decider

logger = logging.getLogger(__name__)

# ---------------------------------- #
# Create Activity Tasks and Decider
# ---------------------------------- #
rs = floto.specs.retry_strategy.InstantRetry(retries=2)
a1 = ActivityTask(name='demo_step1', version='v2', retry_strategy=rs)
a2 = ActivityTask(name='demo_step2', version='v2', requires=[a1], retry_strategy=rs, 
        input={'start_val': 1} )
a2a = ActivityTask(name='demo_step2', version='v2', requires=[a1], retry_strategy=rs, 
        input={'start_val': 2} )
a4 = ActivityTask(name='demo_step4', version='v1', requires=[a2, a2a], retry_strategy=rs)

decider_spec = DeciderSpec(domain='floto_test',
                           task_list='demo_step_decisions',
                           activity_tasks=[a1,a2,a2a,a4],
                           activity_task_list='demo_step_activities',
                           terminate_decider_after_completion=False)

decider = floto.decider.Decider(decider_spec=decider_spec)

# ---------------------------------- #
# Start the decider
# ---------------------------------- #
decider.run()
