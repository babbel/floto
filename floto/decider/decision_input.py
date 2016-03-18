import floto.specs
import json

class DecisionInput:
    def __init__(self, execution_graph=None):
        self.history = None
        self._workflow_input = None
        self._execution_graph = execution_graph

    def get_input_task(self, task, is_failed_task=False):
        if isinstance(task, floto.specs.ActivityTask):
            if is_failed_task:
                return self._get_input_scheduled_task(task.id_, 'ActivityTaskScheduled')
            else:
                return self._get_input(task, 'activity_task')
        elif isinstance(task, floto.specs.ChildWorkflow):
            if is_failed_task:
                return self._get_input_scheduled_task(task.id_, 
                        'StartChildWorkflowExecutionInitiated')
            else:
                return self._get_input(task, 'child_workflow_task')
        else:
            return None

    def get_input_workflow(self):
        if not self._workflow_input:
            self._workflow_input = self.history.get_workflow_input()
        return self._workflow_input

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

    def _get_input(self, task, input_field_name):
        input_ = {}
        dependencies = self._execution_graph.get_dependencies(task.id_)
        if dependencies:
            for d in dependencies:
                # TODO test
                if not isinstance(d, floto.specs.Generator):
                    result = self.history.get_result_completed_activity(d)
                    if result:
                        input_[d.id_] = result
        elif self.get_input_workflow():
            input_['workflow'] = self.get_input_workflow()
        if task.input:
            input_[input_field_name] = task.input
        return input_ if input_ else None


    def _get_input_scheduled_task(self, id_, event_type):
        scheduled_event = self.history.get_event_by_task_id_and_type(id_, event_type)
        attributes = self.history.get_event_attributes(scheduled_event)
        input_ = json.loads(attributes['input']) if 'input' in attributes else None
        return input_

