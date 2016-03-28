import uuid

from test_helper import DeciderEarlyExit
from test_helper import get_result

import floto
import floto.api
from floto.specs import DeciderSpec
from floto.specs.task import ActivityTask, Timer
from floto.specs.retry_strategy import InstantRetry


def test_09():
    rs = InstantRetry(retries=10)
    rs_2 = InstantRetry(retries=2)

    task_1 = ActivityTask(name='activity1', version='v5', retry_strategy=rs_2)
    task_failes_3 = ActivityTask(name='activity_fails_3', version='v2', retry_strategy=rs)
    timer_a = Timer(id_='TimerA', delay_in_seconds=15)

    timer_b = Timer(id_='TimerB', delay_in_seconds=3, requires=[task_1])
    task_2 = ActivityTask(name='activity2', version='v4', requires=[timer_b], retry_strategy=rs_2)
    task_4 = ActivityTask(name='activity4', version='v2', requires=[task_1, task_2],
                          retry_strategy=rs_2)

    tasks = [task_1, task_failes_3, timer_a, timer_b, task_2, task_4]

    decider_spec = DeciderSpec(domain='floto_test',
                               task_list=str(uuid.uuid4()),
                               activity_tasks=tasks,
                               default_activity_task_list='floto_activities',
                               repeat_workflow=True)

    decider = DeciderEarlyExit(repetitions=2, decider_spec=decider_spec)

    workflow_args = {'domain': decider_spec.domain,
                     'workflow_type_name': 'my_workflow_type',
                     'workflow_type_version': 'v1',
                     'task_list': decider_spec.task_list,
                     'input': {'foo': 'bar'}}

    response = floto.api.Swf().start_workflow_execution(**workflow_args)

    print(30 * '-' + ' Running Test 09 ' + 30 * '-')
    decider.run()
    print(30 * '-' + ' Done Test 09    ' + 30 * '-')
    return get_result(decider.domain, response['runId'], 'my_workflow_type_v1')
