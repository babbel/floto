import logging
import random
import socket
import time
import uuid

import floto

logger = logging.getLogger(__name__)

domain = 'floto_test'
activity_task_list = 'demo_step_activities'


# ---------------------------------- #
# Create the activity functions
# ---------------------------------- #
# Register floto activities with floto. The context parameter carries the input information. The context
# parameter is not mandatory.

@floto.activity(domain=domain, name='demo_step1', version='v4')
def step1(context):
    logger.debug('Running step1. Context: {}'.format(context))
    print(context)
    try:
        min_val = context['workflow'].get('min_val')
        max_val = context['workflow'].get('max_val')
    except KeyError:
        min_val = 0
        max_val = 1
        pass

    result = random.randint(min_val, max_val)
    return {'result': result}


@floto.activity(domain=domain, name='demo_step2', version='v4')
def step2(context):
    logger.debug('Running step2. Context: {}'.format(context))
    print(context)

    results_step1 = [v['result'] for k, v in context.items() if 'demo_step1' in k]
    sleeptime = min(results_step1)
    print('step2 will sleep for {} seconds'.format((sleeptime)))
    time.sleep(sleeptime)

    result = random.randint(10, 20)
    return {'result': result}


@floto.activity(domain=domain, name='demo_step3', version='v4')
def step3(context):
    logger.debug('Running step3. Context: {}'.format(context))
    print(context)

    sleeptime = context.get('activity_task', {}).get('start_val', 0)
    print('step3 will sleep for {} * 2 seconds'.format((sleeptime)))
    time.sleep(sleeptime * 2)

    return {'result': sleeptime * 2}


# ---------------------------------- #
# Create Worker
# ---------------------------------- #

identity = socket.getfqdn(socket.gethostname()) + '-' + str(uuid.uuid4())
logger.debug('Starting Worker {}'.format(identity))
worker = floto.ActivityWorker(domain=domain, task_list=activity_task_list,
                              identity=identity,
                              task_heartbeat_in_seconds = 10)
worker.run()
