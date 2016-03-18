import floto
import floto.api
import floto.decisions
import floto.specs
from floto.decider import Base


class Decider(Base):
    """Decider which is defined by a DeciderSpec object 

    Parameters
    ----------
    decider_spec: str (JSON) or floto.specs.DeciderSpec
       For definition of decider spec see floto.specs.DeciderSpec
    """

    def __init__(self, decider_spec=None):
        super().__init__()

        if isinstance(decider_spec, str):
            self.decider_spec = floto.specs.DeciderSpec.from_json(decider_spec)
        else:
            self.decider_spec = decider_spec

        if not (self.decider_spec.domain and self.decider_spec.task_list):
            raise ValueError('Domain or task_list missing in decider spec')

        self.task_list = self.decider_spec.task_list
        self.domain = self.decider_spec.domain

        self.repeat_workflow = self.decider_spec.repeat_workflow
        self.activity_task_list = self.decider_spec.activity_task_list or 'floto_activities'

        activity_tasks = self.decider_spec.activity_tasks
        self.decision_builder = floto.decider.DecisionBuilder(activity_tasks,
                                                              self.activity_task_list)

    def get_decisions(self):
        """Heart of the decider logics. Called by floto.decider.Base in each 
        'poll_for_decision_taks loop'. Fills self.decisions, which are returned to SWF.
        """
        # TODO: redesign desc
        desc = self.get_workflow_execution_description()
        self.decision_builder.current_workflow_execution_description = desc
        self.decisions = self.decision_builder.get_decisions(self.history)
        self.terminate_workflow = self.decision_builder.is_terminate_workflow()

    def tear_down(self):
        """If self.reapeat_workflow is True, the workflow is restarted after successful
        completion."""
        if self.repeat_workflow:
            execution_info = self.get_workflow_execution_description()['executionInfo']
            args = {'domain': self.domain,
                    'workflow_type_name': execution_info['workflowType']['name'],
                    'workflow_type_version': execution_info['workflowType']['version'],
                    'task_list': self.task_list,
                    'input': self.decision_builder.decision_input.get_input_workflow()}
            self.swf.start_workflow_execution(**args)
            self.terminate_workflow = False
            self.terminate_decider = False
        else:
            self.terminate_decider = self.decider_spec.terminate_decider_after_completion

