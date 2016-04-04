import json
import logging

import floto.specs
import floto.specs.serializer

logger = logging.getLogger(__name__)


class DeciderSpec:
    """Specification of a decider. Objects of this class define the execution logic of
    floto.decider.Deciders"""

    def __init__(self, *, domain, task_list, activity_tasks=None,
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

    def serializable(self):
        class_path = floto.specs.serializer.class_path(self)
        cpy = floto.specs.serializer.ordered_dict_with_type(self.__dict__, class_path)

        if cpy.get('activity_tasks'):
            tasks = [t.serializable() for t in cpy['activity_tasks']]
            cpy['activity_tasks'] = tasks
        logger.debug('Serialized DeciderSpec: {}'.format(cpy))
        return cpy

    @classmethod
    def deserialized(cls, **kwargs):
        """Construct an instance from a dict of attributes
        """
        # TODO test
        cpy = floto.specs.serializer.copy_dict(kwargs, filter_keys=['type'])
        if cpy.get('activity_tasks'):
            tasks = []
            for t in cpy['activity_tasks']:
                task = floto.specs.serializer.get_class(t['type'])
                tasks.append(task.deserialized(**t))
            cpy['activity_tasks'] = tasks
        logger.debug('Deserialize DeciderSpec with: {}'.format(cpy))
        return cls(**cpy)

    @classmethod
    def from_json(cls, json_str):
        logger.debug('from json: {}'.format(json_str))
        json_dict =  json.loads(json_str)
        return cls.deserialized(**json_dict)

    def to_json(self):
        json_str = json.dumps(self.serializable(), sort_keys=True)
        logger.debug('to_json: {}'.format(json_str))
        return json_str
