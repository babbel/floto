import logging

import floto
import floto.decider
from floto.specs import DeciderSpec
from floto.specs.task import ActivityTask

logger = logging.getLogger(__name__)
domain = 'floto_test'

# ---------------------------------- #
# Create Activity Tasks and Decider
# ---------------------------------- #
rs = floto.specs.retry_strategy.InstantRetry(retries=2)

a1 = ActivityTask(domain=domain, name='demo_step1', version='v4', retry_strategy=rs)

a2 = ActivityTask(domain=domain, name='demo_step2', version='v4', requires=[a1.id_], retry_strategy=rs)

a3a = ActivityTask(domain=domain, name='demo_step3', version='v4', requires=[a1.id_], retry_strategy=rs, input={'start_val': 5})
a3b = ActivityTask(domain=domain, name='demo_step3', version='v4', requires=[a1.id_], retry_strategy=rs, input={'start_val': 4})
a3c = ActivityTask(domain=domain, name='demo_step3', version='v4', requires=[a1.id_], retry_strategy=rs, input={'start_val': 3})
a3d = ActivityTask(domain=domain, name='demo_step3', version='v4', requires=[a1.id_], retry_strategy=rs, input={'start_val': 2})

a3e = ActivityTask(domain=domain, name='demo_step3', version='v4', requires=[a1.id_, a2.id_, a3a.id_, a3b.id_,
                                                                             a3c.id_, a3d.id_],
                   retry_strategy=rs, input={'start_val': 1})

decider_spec = DeciderSpec(domain='floto_test',
                           task_list='demo_step_decisions',
                           activity_tasks=[a1, a2, a3a, a3b, a3c, a3d, a3e],
                           default_activity_task_list='demo_step_activities',
                           terminate_decider_after_completion=True)

decider = floto.decider.Decider(decider_spec=decider_spec)

# ---------------------------------- #
# Start the decider
# ---------------------------------- #
decider.run()
