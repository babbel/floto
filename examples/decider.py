import uuid

import floto.api
import floto.decider
from floto.specs import ActivityTask, DeciderSpec

#################################################
## Run with:                                   ##
## shell 1: python examples/decider.py         ##
## shell 2: python examples/activity_worker.py ##
#################################################

# Register workflow_type if does not yet exist
workflow_type = floto.api.WorkflowType(domain='floto_test', name='my_workflow_type', version='v1')
# swf.register_workflow_type(workflow_type)

# Define the activity tasks and their dependencies
activity_task_2 = ActivityTask(name='activity2', version='v1')
activity_task_1 = ActivityTask(name='activity1', version='v1', requires=[activity_task_2],
                               input={'task_input': '4'})
# activity_task_1 = ActivityTask(name='activity1', version='v1')

rs = floto.specs.retry_strategy.InstantRetry(retries=5)
activity_task_fail = ActivityTask(name='activity_fails_3', version='v1', retry_strategy=rs)

# Create a decider spec
# activity_tasks = [activity_task_fail]
activity_tasks = [activity_task_1, activity_task_2]
decider_spec = DeciderSpec(domain='floto_test',
                           activity_tasks=activity_tasks,
                           task_list='your_decider_task_list',
                           activity_task_list='floto_activities')

task_list = str(uuid.uuid4())
decider = floto.decider.Decider(decider_spec=decider_spec)

print(decider_spec.to_json())
# Start workflow execution
swf = floto.api.Swf()
# swf.start_workflow_execution(domain='floto_test',
# workflow_type_name=workflow_type.name,
# workflow_type_version=workflow_type.version,
# task_list=task_list,
# input={'foo':'bar'})

decider.run()
try:
    # Decider runs in separate process and terminates when all activity tasks have finished
    decider.run()
except Exception as e:
    print(e)
    swf.terminate_workflow_execution(domain='floto_test', workflow_id='my_workflow_type_v1')
