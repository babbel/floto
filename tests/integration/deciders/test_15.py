import time
import uuid

from test_helper import get_result

import floto
import floto.api
import floto.decider
from floto.specs import DeciderSpec
from floto.specs.task import ActivityTask, ChildWorkflow
from floto.specs.retry_strategy import InstantRetry


def decider_spec_child_workflow():
    rs = InstantRetry(retries=1)
    task_1 = ActivityTask(domain='floto_test', name='activity1', version='v5', retry_strategy=rs)
    decider_spec = DeciderSpec(domain='floto_test',
                               task_list='child_workflow_task_list',
                               activity_tasks=[task_1],
                               default_activity_task_list='floto_activities',
                               terminate_decider_after_completion=True)
    return decider_spec

def decider_spec_workflow():
    rs = InstantRetry(retries=1)
    task_1 = ActivityTask(domain='floto_test', name='activity1', version='v5', retry_strategy=rs)
    child_workflow = ChildWorkflow(domain='floto_test', workflow_type_name='test_child_workflow', 
            workflow_type_version='v2', retry_strategy=rs, task_list='child_workflow_task_list',
            requires=[task_1.id_])
    decider_spec = DeciderSpec(domain='floto_test',
                               task_list=str(uuid.uuid4()),
                               activity_tasks=[task_1, child_workflow],
                               default_activity_task_list='floto_activities',
                               terminate_decider_after_completion=True)
    return decider_spec

def test_15():
    decider_workflow = floto.decider.Decider(decider_spec=decider_spec_workflow())
    decider_child_workflow = floto.decider.Decider(decider_spec=decider_spec_child_workflow())

    workflow_args = {'domain': decider_workflow.domain,
                     'workflow_type_name': 'my_workflow_type',
                     'workflow_type_version': 'v1',
                     'task_list': decider_workflow.task_list,
                     'input':{'foo':'bar'}}

    response = floto.api.Swf().start_workflow_execution(**workflow_args)
    run_id = response['runId']
    workflow_id = 'my_workflow_type_v1'

    print(30 * '-' + ' Running Test 15 ' + 30 * '-')
    decider_child_workflow.run(separate_process=True)
    decider_workflow.run()
    result = get_result(decider_workflow.domain, run_id, workflow_id)
    print(30 * '-' + ' Done Test 15    ' + 30 * '-')

    return result
