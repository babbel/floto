import floto.api
from floto.specs import ActivityTask, DeciderSpec

activity_task_2 = ActivityTask(name='activity2', version='v1')
activity_task_1 = ActivityTask(name='activity1', version='v1', requires=[activity_task_2])

decider_spec = DeciderSpec(activity_tasks=[activity_task_1, activity_task_2])
child_workflow_spec = {'decider_spec': decider_spec}

# Send a signal to the daemon and initiate a child workflow
floto.api.Swf().signal_workflow_execution(domain='floto_test', workflow_id='floto_daemon',
                                          signal_name='startChildWorkflowExecution',
                                          input=child_workflow_spec)
