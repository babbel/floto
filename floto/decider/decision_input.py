import floto.specs
import json

class DecisionInput:
    def __init__(self, execution_graph=None):
        self.history = None
        self._execution_graph = execution_graph

    def get_input_task(self, task, is_failed_task=False):
        if is_failed_task:
            event_type = None
            if isinstance(task, floto.specs.task.ActivityTask):
                event_type = 'ActivityTaskScheduled'
            elif isinstance(task, floto.specs.task.ChildWorkflow):
                event_type = 'StartChildWorkflowExecutionInitiated'
            return self._get_input_scheduled_task(task.id_, event_type) 
        else:
            return self._get_input(task)

    def get_input_workflow(self):
        return self.history.get_workflow_input()

    def get_details_failed_tasks(self, failed_tasks_events):
        details = {}
        for e in failed_tasks_events:
            attributes = self.history.get_event_attributes(e)
            if 'details' in attributes:
                id_ = self.history.get_id_task_event(e)
                details[id_] = floto.specs.JSONEncoder.load_string(attributes['details'])
        return details

    def get_workflow_result(self):
        outgoing_vertices = self._execution_graph.outgoing_vertices()
        result = {}
        for task in outgoing_vertices:
            r = self.history.get_result_completed_activity(task)
            if r:
                result[task.id_] = r
        return result if result else None

    def _get_input(self, task):
        """Gets the input for <task>. If task has dependencies the result of the dependencies are
        added to the input. If task does not have dependencies, the workflow input is added. If the
        task itself was provided with input at the task definition the task input is added with the
        key 'activity_task/child_workflow_task'. 
        """
        input_ = {}
        dependencies = self._execution_graph.get_dependencies(task.id_)
        if dependencies:
            for d in dependencies:
                if not isinstance(d, floto.specs.task.Generator):
                    result = self.history.get_result_completed_activity(d)
                    if result:
                        input_[d.id_] = result
        elif self.get_input_workflow():
            input_workflow = self._remove_activity_tasks(self.get_input_workflow())
            if input_workflow:
                input_['workflow'] = self._remove_activity_tasks(self.get_input_workflow())

        if task.input and isinstance(task, floto.specs.task.ChildWorkflow): 
            input_['child_workflow_task'] = task.input
        elif task.input and isinstance(task, floto.specs.task.ActivityTask):
            input_['activity_task'] = self._remove_activity_tasks(task.input)

        return input_ if input_ else None

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

    def _get_input_scheduled_task(self, id_, event_type):
        scheduled_event = self.history.get_event_by_task_id_and_type(id_, event_type)
        attributes = self.history.get_event_attributes(scheduled_event)
        input_ = json.loads(attributes['input']) if 'input' in attributes else None
        return input_

