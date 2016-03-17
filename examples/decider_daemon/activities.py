import floto
import logging
import random

logger = logging.getLogger(__name__)

domain = 'floto_test'
activity_task_list = 'demo_step_activities'

# ---------------------------------- #
# Create the activity functions
# ---------------------------------- #
# Register floto activities. The context parameter carries the input information. The context
# parameter is not mandatory.
@floto.activity(name='demo_step1', version='v2')
def step1(context):
    logger.debug('Running set_random_number. Context: ', context)
    print(context)
    try:
        startval = context['workflow'].get('start_val')
    except KeyError:
        startval = 0
        pass

    result = startval + random.randint(5, 60)
    return {'result': result}


@floto.activity(name='demo_step2', version='v2')
def step2(context):
    logger.debug('Running add_random_number. Context: ', context)
    print(context)
    result = random.randint(0, 10000)
    return {'result': result}

@floto.activity(name='demo_step3', version='v2')
def step3(context):
    logger.debug('Running add_random_number. Context: ', context)
    print(context)
    result = random.randint(3, 9)
    return {'result': result}

@floto.activity(name='demo_step4', version='v1')
def step4(context):
    logger.debug('Generating spec for child_workflow. Context: ', context)

    results_step2 = [v['result'] for k,v in context.items() if 'demo_step2' in k]

    activity_tasks = [floto.specs.ActivityTask(name='demo_step2', version='v2', 
        input={'start_val':start_val}) for start_val in results_step2]

    decider_spec = floto.specs.DeciderSpec(domain='floto_test',
             activity_task_list=activity_task_list,
             activity_tasks=activity_tasks)

    floto.api.Swf().signal_workflow_execution(domain='floto_test', 
            workflow_id='floto_daemon',
            signal_name='startChildWorkflowExecution',
            input={'decider_spec':decider_spec})

    return {'status':'success'}


worker_1 = floto.ActivityWorker(domain=domain, task_list=activity_task_list)
worker_1.run()
