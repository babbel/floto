import uuid
import time

from test_helper import is_workflow_completed, SlowDecider

import floto
import floto.api
import floto.decider
from floto.specs import DeciderSpec
from floto.specs.task import ActivityTask, Generator

def test_17():
    rs = floto.specs.retry_strategy.InstantRetry(retries=2)
    generator_task_1 = Generator(name='generator1', version='v1', retry_strategy=rs) 
    task_2 = ActivityTask(name='activity6', version='v1', requires=[generator_task_1])
    
    decider_spec = DeciderSpec(domain='floto_test',
                               task_list=str(uuid.uuid4()),
                               activity_tasks=[generator_task_1, task_2],
                               activity_task_list='floto_activities',
                               terminate_decider_after_completion=True)

    decider_1 = floto.decider.Decider(decider_spec=decider_spec)
    decider_2 = SlowDecider(decider_spec=decider_spec, timeout=20, num_timeouts=2)

    response = floto.api.Swf().start_workflow_execution(domain='floto_test', 
                                   workflow_type_name='decider_timeout_workflow',
                                   workflow_type_version='v2',
                                   task_list=decider_spec.task_list,
                                   input={'foo':'bar'})
    run_id = response['runId']
    workflow_id = 'decider_timeout_workflow_v2'

    print(30*'-'+' Running Test 17 '+30*'-')
    decider_1.run(separate_process=True)
    decider_2.run(separate_process=True)

    result = None
    while True:
        time.sleep(5)
        result = is_workflow_completed(decider_1.domain, response['runId'], workflow_id)
        if result:
            decider_1._separate_process.terminate()
            decider_2._separate_process.terminate()
            break
    print(30*'-'+' Done Test 17    '+30*'-')
    return result 


