import json

from floto.specs.task import Task


class ActivityTask(Task):
    def __init__(self, name=None, version=None, activity_id=None, requires=None, input=None,
                 retry_strategy=None, task_list=None):
        """Defines an activity task which is used in decider specs.

        Parameters
        ----------
        name: str [Required]
        version: str [Required]
        activity_id: str
            The id of the activity task. Defaults to: <name>:<version>:hash(input/requires)
        requires: list
            List of activity tasks this activity task depends on
        input: dict
        retry_strategy: floto.specs.Strategy
        """
        super().__init__(requires=requires)

        self.name = name
        self.version = version
        self.input = input
        self.id_ = activity_id or self._default_id(name, version, input)
        self.retry_strategy = retry_strategy
        self.task_list = task_list

