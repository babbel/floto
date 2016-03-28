import uuid

import floto.decider
from floto.decider import Base


class Daemon(Base):
    def __init__(self, domain=None, task_list=None, swf=None):
        super().__init__(swf=swf)
        self.task_list = task_list or 'floto_daemon'
        if domain:
            self.domain = domain

    def get_decisions(self):
        signals = self.history.get_events_up_to_last_decision('WorkflowExecutionSignaled')
        self.decisions.extend(self.get_decisions_child_workflows(signals))

    def get_decisions_child_workflows(self, signal_events):
        decisions = []
        for signal in signal_events:
            decision = self.get_decision_child_workflow(signal)
            if decision: decisions.append(decision)
        return decisions

    def get_decision_child_workflow(self, signal_event):
        decision = None
        attributes = self.history.get_event_attributes(signal_event)

        if attributes['signalName'] == 'startChildWorkflowExecution':
            decision = self.get_decision_start_child_workflow_execution()
            if 'input' in attributes:
                input_ = floto.specs.JSONEncoder.load_string(attributes['input'])
                if 'decider_spec' in input_:
                    json_decider_spec = floto.specs.JSONEncoder.dump_object(input_['decider_spec'])
                    decider_spec = self.get_decider_spec(json_decider_spec,
                                                         decision.task_list['name'],
                                                         self.domain)
                    self.start_child_decider(decider_spec)
        return decision

    def get_decision_start_child_workflow_execution(self):
        child_workflow_type = floto.api.WorkflowType(domain='d', name='child_workflow', version='v1')
        child_workflow_id = str(uuid.uuid4())
        child_workflow_task_list = 'task_list_{}'.format(child_workflow_id)

        child_workflow = floto.decisions.StartChildWorkflowExecution()
        child_workflow.workflow_id = child_workflow_id
        child_workflow.workflow_type = child_workflow_type
        child_workflow.task_list = {'name': child_workflow_task_list}
        return child_workflow

    def start_child_decider(self, decider_spec):
        decider = floto.decider.Decider(decider_spec=decider_spec)
        decider.run(separate_process=True)

    def get_decider_spec(self, json_decider_spec, task_list, domain):
        decider_spec = floto.specs.DeciderSpec.from_json(json_decider_spec)
        decider_spec.task_list = task_list
        decider_spec.domain = domain
        return decider_spec
