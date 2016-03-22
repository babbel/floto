import uuid

from test_helper import DeciderEarlyExit
from test_helper import get_result

import floto
import floto.api


def test_08():
    rs = floto.specs.retry_strategy.InstantRetry(retries=2)
    activity_task_1 = floto.specs.task.ActivityTask(name='activity1', version='v5', 
            retry_strategy=rs)

    decider_spec = floto.specs.DeciderSpec(domain='floto_test',
                                           task_list=str(uuid.uuid4()),
                                           activity_tasks=[activity_task_1],
                                           default_activity_task_list='floto_activities',
                                           repeat_workflow=True)

    decider = DeciderEarlyExit(repetitions=2, decider_spec=decider_spec)

    workflow_args = {'domain': decider_spec.domain,
                     'workflow_type_name': 'my_workflow_type',
                     'workflow_type_version': 'v1',
                     'task_list': decider_spec.task_list,
                     'input': {'foo': 'bar'}}

    response = floto.api.Swf().start_workflow_execution(**workflow_args)

    print(30 * '-' + ' Running Test 08 ' + 30 * '-')
    decider.run()
    print(30 * '-' + ' Done Test 08    ' + 30 * '-')
    return get_result(decider.domain, response['runId'], 'my_workflow_type_v1')
