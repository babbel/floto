import uuid

from test_helper import get_result, SlowDecider

import floto
import floto.api
from floto.specs import DeciderSpec
from floto.specs.task import ActivityTask
from floto.specs.retry_strategy import InstantRetry


def test_11():
    rs = InstantRetry(retries=2)
    activity_task_1 = ActivityTask(name='activity1', version='v5', retry_strategy=rs)

    decider_spec = DeciderSpec(domain='floto_test',
                               task_list=str(uuid.uuid4()),
                               activity_tasks=[activity_task_1],
                               default_activity_task_list='floto_activities',
                               terminate_decider_after_completion=True)

    decider = SlowDecider(decider_spec)

    response = floto.api.Swf().start_workflow_execution(domain='floto_test', 
                                   workflow_type_name='decider_timeout_workflow',
                                   workflow_type_version='v1',
                                   task_list=decider_spec.task_list,
                                   input={'foo':'bar'})
    run_id = response['runId']
    workflow_id = 'decider_timeout_workflow_v1' 

    print(30*'-'+' Running Test 11 '+30*'-')
    decider.run()
    print(30*'-'+' Done Test 11    '+30*'-')
    return get_result('floto_test', run_id, workflow_id)    


