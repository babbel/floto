import floto
from floto.specs import ActivityTask, DeciderSpec
import random


# Register floto activities. The context parameter carries the input information. The context
# parameter It is not mandatory. 
@floto.activity(name='activity1', version='v1')
def activity1(context):
    print('Running activity1')
    print(context)
    return {'activity_id': 'activity1:v1'}


@floto.activity(name='activity2', version='v1')
def activity2():
    print('Running activity2')
    return {'activity_id': 'activity2:v1'}


FAILURES = 3
FAILURE_COUNT = 0


@floto.activity(name='activity_fails_3', version='v1')
def activity_fails_3():
    print('Running activity_fails_3')
    global FAILURE_COUNT
    global FAILURES

    if FAILURE_COUNT < FAILURES:
        FAILURE_COUNT += 1
        raise Exception('Something went wrong')
    else:
        FAILURE_COUNT = 0

        # @floto.activity(name='date_generator', version='v1')
        # def date_generator(context):
        # activity_task_1 = ActivityTask(name='activity1', version='v1', input={'date':1})
        # activity_task_2 = ActivityTask(name='activity2', version='v1', requires=[activity_task_1])
        # activity_tasks = [activity_task_1, activity_task_2]
        # decider_spec = DeciderSpec(activity_tasks=activity_tasks)

        # floto.api.Swf().signal_workflow_execution(domain='floto_test', workflow_id='floto_runner',
        # signal_name='startChildWorkflowExecution',
        # input={'decider_spec':decider_spec})


worker = floto.ActivityWorker(domain='floto_test', task_list='floto_activities')
worker.run()
