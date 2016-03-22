import floto
from floto.decider import Decider
import json

class DynamicDecider(Decider):
    """DynamicDecider reads the execution logic defined by activity tasks from the workflow 
    input."""
    def __init__(self, decider_spec, identity=None):
        super().__init__(decider_spec=decider_spec, identity=identity)

    def get_decisions(self):
        input_ = self.history.get_workflow_input()
        activity_tasks = self.get_activity_tasks_from_input(input_)
        print(input_)
        print(activity_tasks)
        self.decision_builder = floto.decider.DecisionBuilder(activity_tasks,
                self.default_activity_task_list)

        self.decisions = self.decision_builder.get_decisions(self.history)
        self.terminate_workflow = self.decision_builder.is_terminate_workflow()

    def get_activity_tasks_from_input(self, input_):
        if isinstance(input_, dict):
            for k,v in input_.items():
                if k == 'activity_tasks':
                    tasks_json = json.dumps(v)
                    tasks = json.loads(tasks_json, object_hook=floto.specs.JSONEncoder.object_hook)
                    return tasks
                else:
                    return self.get_activity_tasks_from_input(v)
        else:
            return None

