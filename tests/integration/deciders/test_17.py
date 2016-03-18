import uuid

from test_helper import get_result

import floto
import floto.api
import floto.decider
from floto.specs import DeciderSpec, ActivityTask, Generator

def test_17():
    rs = floto.specs.retry_strategy.InstantRetry(retries=2)
    generator_task_1 = Generator(name='generator1', version='v1', retry_strategy=rs) 
    task_2 = ActivityTask(name='activity6', version='v1', requires=[generator_task_1])
    
    decider_spec = DeciderSpec(domain='floto_test',
                               task_list=str(uuid.uuid4()),
                               activity_tasks=[generator_task_1, task_2],
                               activity_task_list='floto_activities',
                               terminate_decider_after_completion=True)

    decider = floto.decider.Decider(decider_spec=decider_spec)

    response = floto.api.Swf().start_workflow_execution(domain='floto_test', 
                                   workflow_type_name='my_workflow_type',
                                   workflow_type_version='v1',
                                   task_list=decider_spec.task_list,
                                   input={'foo':'bar'})
    run_id = response['runId']
    workflow_id = 'my_workflow_type_v1'

    print(30*'-'+' Running Test 17 '+30*'-')
    decider.run()
    print(30*'-'+' Done Test 17    '+30*'-')
    return get_result('floto_test', run_id, workflow_id)    


