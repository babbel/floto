import json

import floto.specs


class DeciderSpec:
    """Specification of a decider. Objects of this class define the execution logics of 
    floto.decider.Deciders"""

    def __init__(self, domain=None, task_list=None, activity_tasks=None, 
            default_activity_task_list=None, repeat_workflow=False, 
            terminate_decider_after_completion=False):
        """
        Parameters
        ----------
        domain: str [Required]
        task_list: str [Required]
            The decider's task list
        activity_tasks: list [Required]
           List of floto.specs.ActivityTasks
        activity_task_list: str
           The activities' task list
        repeat_workflow: bool
            If True, the workflow execution will be repeated after completion
        """
        self.domain = domain
        self.task_list = task_list
        self.activity_tasks = activity_tasks
        self.default_activity_task_list = default_activity_task_list
        self.repeat_workflow = repeat_workflow
        self.terminate_decider_after_completion = terminate_decider_after_completion

    def to_json(self):
        return json.dumps(self, cls=floto.specs.JSONEncoder, sort_keys=True)

    @staticmethod
    def from_json(json_str):
        return json.loads(json_str, object_hook=floto.specs.JSONEncoder.object_hook)
