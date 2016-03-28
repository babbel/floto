import json
import logging

import floto.specs

logger = logging.getLogger(__name__)


class DeciderSpec:
    """Specification of a decider. Objects of this class define the execution logic of
    floto.decider.Deciders"""

    def __init__(self, *, domain, task_list, activity_tasks,
                 default_activity_task_list=None, repeat_workflow=False,
                 terminate_decider_after_completion=False):
        """
        Parameters
        ----------
        domain: str
        task_list: str
            The decider's task list
        activity_tasks: list[floto.specs.ActivityTask]
           List of floto.specs.ActivityTasks
        default_activity_task_list: Optional[str]
           The activities' task list
        repeat_workflow: Optional[bool]
            If True, the workflow execution will be repeated after completion
        """
        self.domain = domain
        self.task_list = task_list
        self.activity_tasks = activity_tasks
        self.default_activity_task_list = default_activity_task_list
        self.repeat_workflow = repeat_workflow
        self.terminate_decider_after_completion = terminate_decider_after_completion

    @classmethod
    def deserialized(cls, **kwargs):
        """Construct an instance from a dict of attributes
        """
        # Remove 'type' key, just in case there still is one
        kwargs.pop('type', None)
        return cls(**kwargs)

    @staticmethod
    def from_json(json_str):
        logger.debug('from json: {}'.format(json_str))
        return json.loads(json_str, object_hook=floto.specs.JSONEncoder.floto_object_hook)

    def to_json(self):
        json_str = json.dumps(self, cls=floto.specs.JSONEncoder, sort_keys=True)
        logger.debug('to_json: {}'.format(json_str))
        return json_str
