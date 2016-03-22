import uuid

from test_helper import get_result

import floto
import floto.api
import floto.decider


def test_07():
    rs = floto.specs.retry_strategy.InstantRetry(retries=2)
    timer_a = floto.specs.task.Timer(id_='TimerA', delay_in_seconds=15)
    task_1 = floto.specs.task.ActivityTask(name='activity1', version='v5', retry_strategy=rs)
    timer_b = floto.specs.task.Timer(id_='TimerB', delay_in_seconds=5, requires=[task_1])
    task_2 = floto.specs.task.ActivityTask(name='activity2', version='v4', requires=[timer_b],
                                      retry_strategy=rs)

    decider_spec = floto.specs.DeciderSpec(domain='floto_test',
                                           task_list=str(uuid.uuid4()),
                                           activity_tasks=[timer_a,
                                                           timer_b,
                                                           task_1,
                                                           task_2],
                                           default_activity_task_list='floto_activities',
                                           terminate_decider_after_completion=True)

    decider = floto.decider.Decider(decider_spec=decider_spec)

    workflow_args = {'domain': decider_spec.domain,
                     'workflow_type_name': 'my_workflow_type',
                     'workflow_type_version': 'v1',
                     'task_list': decider_spec.task_list,
                     'input': {'foo': 'bar'}}

    response = floto.api.Swf().start_workflow_execution(**workflow_args)

    print(30 * '-' + ' Running Test 07 ' + 30 * '-')
    decider.run()
    print(30 * '-' + ' Done Test 07    ' + 30 * '-')
    return get_result(decider.domain, response['runId'], 'my_workflow_type_v1')
