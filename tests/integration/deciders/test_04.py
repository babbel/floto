import uuid

from test_helper import get_result

import floto
import floto.api
import floto.decider
from floto.specs import DeciderSpec
from floto.specs.task import ActivityTask


def test_04():
    domain = 'floto_test'
    rs = floto.specs.retry_strategy.InstantRetry(retries=2)
    a1 = ActivityTask(domain=domain, name='activity1', version='v5', retry_strategy=rs) 
    a3 = ActivityTask(domain=domain, name='activity3', version='v2', requires=[a1.id_], 
            retry_strategy=rs ) 

    decider_spec = DeciderSpec(domain=domain,
                               task_list=str(uuid.uuid4()),
                               activity_tasks=[a1, a3],
                               default_activity_task_list='floto_activities',
                               terminate_decider_after_completion=True)

    decider = floto.decider.Decider(decider_spec=decider_spec.to_json())

    workflow_args = {'domain':decider_spec.domain,
                     'workflow_type_name':'my_workflow_type',
                     'workflow_type_version':'v1',
                     'task_list':decider_spec.task_list,
                     'input':{'foo':'bar'}}

    response = floto.api.Swf().start_workflow_execution(**workflow_args)

    print(30*'-'+' Running Test 04 '+30*'-')
    decider.run()
    print(30*'-'+' Done Test 04    '+30*'-')
    return get_result(decider.domain, response['runId'], 'my_workflow_type_v1')    


