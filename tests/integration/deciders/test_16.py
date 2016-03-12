import time
import uuid

from test_helper import get_fail_workflow_execution

import floto
import floto.api
import floto.decider
from floto.specs import ActivityTask, DeciderSpec, ChildWorkflow
from floto.specs.retry_strategy import InstantRetry


def decider_spec_child_workflow():
    task_1 = ActivityTask(name='activity_fails_2', version='v2')
    decider_spec = DeciderSpec(domain='floto_test',
                               task_list='child_workflow_task_list',
                               activity_tasks=[task_1],
                               activity_task_list='floto_activities',
                               repeat_workflow=False)
    return decider_spec

def decider_spec_workflow():
    rs = InstantRetry(retries=1)
    child_workflow = ChildWorkflow(workflow_type_name='test_child_workflow', 
            workflow_type_version='v2', retry_strategy=rs, task_list='child_workflow_task_list')
    decider_spec = DeciderSpec(domain='floto_test',
                               task_list=str(uuid.uuid4()),
                               activity_tasks=[child_workflow],
                               terminate_decider_after_completion=True)
    return decider_spec

def test_16():
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

    print(30 * '-' + ' Running Test 16 ' + 30 * '-')
    decider_child_workflow.run(separate_process=True)
    decider_workflow.run()
    decider_child_workflow._separate_process.terminate()
    result = get_fail_workflow_execution(decider_workflow.domain, run_id, workflow_id)
    print(30 * '-' + ' Done Test 16    ' + 30 * '-')

    return result
