import os 
import yaml
from multiprocessing import Process
import time

import floto
import floto.api

import floto.decider
import floto.specs
import floto.specs.retry_strategy
import floto
import floto.decorators

# Set up the logger
def configure_logger(path):
    import logging.config

    if path.endswith('.yml'):
        if os.path.exists(path):
            with open(path, 'rt') as f:
                config = yaml.safe_load(f.read())
            logging.config.dictConfig(config)

filename = 'conf.template.yml'
cwd = os.path.dirname(os.path.realpath(__file__))
path = os.path.join(cwd, '..', 'logging', filename)
configure_logger(path)

########################
### Set up the types ###
########################

swf = floto.api.Swf()

# Register a domain
domain = 'floto_test'
swf.domains.register_domain(domain)

# Define and register a workflow type.
workflow_type = floto.api.WorkflowType(domain=domain, 
                                       name='my_workflow_type', 
                                       version='v2', 
                                       default_task_start_to_close_timeout='20')
swf.register_workflow_type(workflow_type)

# Define and register an activity type
activity_type = floto.api.ActivityType(domain=domain, 
                                       name='simple_activity', 
                                       version='v2',
                                       default_task_heartbeat_timeout='20')
swf.register_activity_type(activity_type)


################################################
### Create a task and the decider and run it ###
################################################
retry_strategy = floto.specs.retry_strategy.InstantRetry(retries=1)
simple_task = floto.specs.task.ActivityTask(domain=domain, 
                                            name='simple_activity', 
                                            version='v2', 
                                            retry_strategy=retry_strategy)

decider_spec = floto.specs.DeciderSpec(domain=domain,
                                       task_list='simple_decider',
                                       default_activity_task_list='hello_world_atl',
                                       terminate_decider_after_completion=True,
                                       activity_tasks=[simple_task])

decider = floto.decider.Decider(decider_spec=decider_spec)
decider.run(separate_process=True)

#############################################
### Create an activity and start a worker ###
#############################################

@floto.activity(domain=domain, name='simple_activity', version='v2')
def simple_activity():
    print('\nSimpleWorker: I\'m working!')
    for i in range(3):
        print('.')
        time.sleep(0.8)

    # Terminate the worker after first execution:
    print('I\'m done.')


def start_worker():
    worker = floto.ActivityWorker(domain=domain, 
                                  task_list='hello_world_atl',
                                  task_heartbeat_in_seconds=10,
                                  identity='HelloWorldWorker')
    worker.run()


if __name__ == '__main__':
    print('\nStarting worker in different process.')
    worker = Process(target=start_worker)
    worker.start()

    print('Starting workflow...')
    swf.start_workflow_execution(domain=domain,
                                 workflow_type_name=workflow_type.name,
                                 workflow_type_version=workflow_type.version,
                                 task_list='simple_decider')

    # Wait for the workflow to finish
    decider._separate_process.join()
    worker.terminate()

