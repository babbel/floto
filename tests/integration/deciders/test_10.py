import uuid

from test_helper import get_result

import floto
import floto.api
import floto.decider
from floto.specs import ActivityTask, DeciderSpec


def test_10():
    rs = floto.specs.retry_strategy.InstantRetry(retries=7)
    activity_task = ActivityTask(name='activity5', version='v1', retry_strategy=rs)

    decider_spec = DeciderSpec(domain='floto_test',
                               task_list=str(uuid.uuid4()),
                               activity_tasks=[activity_task],
                               activity_task_list='floto_activities')

    decider = floto.decider.Decider(decider_spec=decider_spec)

    workflow_args = {'domain': decider_spec.domain,
                     'workflow_type_name': 'my_workflow_type',
                     'workflow_type_version': 'v1',
                     'task_list': decider_spec.task_list}

    response = floto.api.Swf().start_workflow_execution(**workflow_args)

    print(30 * '-' + ' Running Test 10 ' + 30 * '-')
    decider.run()
    print(30 * '-' + ' Done Test 10    ' + 30 * '-')
    return get_result(decider.domain, response['runId'], 'my_workflow_type_v1')
