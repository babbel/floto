import floto
from floto.decider import Decider
import json

class DynamicDecider(Decider):
    def __init__(self, decider_spec):
        super().__init__(decider_spec=decider_spec)

    def get_decisions(self):
        if not self.decision_builder:
            spec = self.get_decider_spec_from_input()
            self.decision_builder = floto.decider.DecisionBuilder(spec.activity_tasks,
                    self.activity_task_list)

        self.decisions = self.decision_builder.get_decisions(self.history)
        self.terminate_workflow = self.decision_builder.is_terminate_workflow()

    def tear_down(self):
        self.terminate_decider = False
        self.decision_builder = None

    def get_decider_spec_from_input(self):
        input_ = self.history.get_workflow_input()
        for values in input_.values():
            for k,v in values.items():
                if 'decider_spec' in k:
                    return floto.specs.DeciderSpec.from_json(v)

