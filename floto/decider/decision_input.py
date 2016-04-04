import logging
import floto.specs
import json

logger = logging.getLogger(__name__)

class DecisionInput:
    def __init__(self):
        self.history = None

    #def get_input_task(self, task, is_failed_task=False):
        #if is_failed_task:
            #event_type = None
            #if isinstance(task, floto.specs.task.ActivityTask):
                #event_type = 'ActivityTaskScheduled'
            #elif isinstance(task, floto.specs.task.ChildWorkflow):
                #event_type = 'StartChildWorkflowExecutionInitiated'
            #return self._get_input_scheduled_task(task.id_, event_type) 
        #else:
            #return self._get_input(task)

    def get_input(self, task, task_input_key, required_tasks):
        """Gets the input for <task>. If task has dependencies the result of the dependencies are
        added to the input. If task does not have dependencies, the workflow input is added. If the
        task itself was provided with input at the task definition the task input is added with the
        key 'activity_task/child_workflow_task'. 
        """
        input_ = {}
        if required_tasks:
            input_.update(self.collect_results(required_tasks))
        else:
            input_workflow = self._remove_activity_tasks(self.history.get_workflow_input())
            if input_workflow:
                input_['workflow'] = input_workflow 

        if task.input:
            input_[task_input_key] = task.input

        return input_ if input_ else None

    def get_details_failed_tasks(self, failed_tasks_events):
        details = {}
        for e in failed_tasks_events:
            attributes = self.history.get_event_attributes(e)
            if 'details' in attributes:
                id_ = self.history.get_id_task_event(e)
                details[id_] = floto.specs.JSONEncoder.load_string(attributes['details'])
        return details

    def collect_results(self, tasks):
        logger.debug('Collection results for {}'.format(tasks))
        result = {}
        for task in tasks:
            if not isinstance(task, floto.specs.task.Generator):
                r = self.history.get_result_completed_activity(task)
                if r:
                    result[task.id_] = r
        return result

    def _remove_activity_tasks(self, input_):
        if isinstance(input_, dict):
            new_input = {}
            for k,v in input_.items():
                if not 'activity_tasks' in k:
                    new_value = self._remove_activity_tasks(v)
                    if new_value:
                        new_input[k] = new_value
            return new_input
        else:
            return input_

