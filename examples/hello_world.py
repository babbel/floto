from multiprocessing import Process
import time

import floto.api

import floto.decider
import floto.specs
import floto
import floto.decorators

########################
### Set up the types ###
########################

swf = floto.api.Swf()

# Register a domain
swf.domains.register_domain('floto_test')

# Define and register a workflow type.
workflow_type = floto.api.WorkflowType(domain='floto_test', name='my_workflow_type', version='v1')
swf.register_workflow_type(workflow_type)

# Define and register an activity type
activity_type = floto.api.ActivityType(domain='floto_test', name='simple_activity', version='v1')
swf.register_activity_type(activity_type)


################################################
### Create a task and the decider and run it ###
################################################

simple_task = floto.specs.task.ActivityTask(name='simple_activity', version='v1')
decider_spec = floto.specs.DeciderSpec(domain='floto_test',
                                       task_list='simple_decider',
                                       default_activity_task_list='hello_world_atl',
                                       terminate_decider_after_completion=True,
                                       activity_tasks=[simple_task])

decider = floto.decider.Decider(decider_spec=decider_spec)
decider.run(separate_process=True)

#############################################
### Create an activity and start a worker ###
#############################################

@floto.activity(name='simple_activity', version='v1')
def simple_activity():
    print('\nSimpleWorker: I\'m working!')
    for i in range(3):
        print('.')
        time.sleep(0.8)

    # Terminate the worker after first execution:
    print('I\'m done.')


def start_worker():
    worker = floto.ActivityWorker(domain='floto_test', task_list='hello_world_atl')
    worker.run()


if __name__ == '__main__':
    print('\nStarting worker in different process.')
    worker = Process(target=start_worker)
    worker.start()

    print('Starting workflow...')
    swf.start_workflow_execution(domain='floto_test',
                                 workflow_type_name=workflow_type.name,
                                 workflow_type_version=workflow_type.version,
                                 task_list='simple_decider')

    # Wait for the workflow to finish
    decider._separate_process.join()
    worker.terminate()

