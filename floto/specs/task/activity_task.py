import logging

from floto.specs.task import Task
import floto.specs

logger = logging.getLogger(__name__)


class ActivityTask(Task):
    def __init__(self, *, domain, name, version, activity_id=None, requires=None, input=None,
                 retry_strategy=None, task_list=None):
        """Defines an activity task which is used in decider specs.

        Parameters
        ----------
        name: str
        version: str
        activity_id: Optional[str]
            The id of the activity task. Defaults to: <name>:<version>:hash(input/requires)
        requires: Optional[list]
            List of activity tasks this activity task depends on
        input: Optional[dict]
        retry_strategy: Optional[floto.specs.Strategy]
        task_list : Optional[str]

        """
        super().__init__(requires=requires)

        self.domain = domain
        self.name = name
        self.version = version
        self.input = input
        self.id_ = activity_id or self._default_id(domain=domain, name=name, version=version, input=input)
        self.retry_strategy = retry_strategy
        self.task_list = task_list


    def serializable(self):
        cpy = super().serializable()

        retry_strategy = cpy.get('retry_strategy')
        if retry_strategy:
            cpy['retry_strategy'] = retry_strategy.serializable()

        logger.debug('serialized ActivityTask: {}'.format(cpy))
        return cpy

    @classmethod
    def _deserialized(cls, **kwargs):
        """Construct an instance from a dict of attributes

        Notes
        -----
        Note that the parameter `activity_id` is assigned to the attribute `id_`

        Examples
        --------
        >>> attrs = {'domain': 'd', 'id_': 'activity1:v1:d:9ba07c7e21', 'input': {'date': 1},
                     'name': 'activity1', 'type': 'floto.specs.task.ActivityTask', 'version': 'v1'}
        >>> cls = floto.specs.task.ActivityTask
        >>> obj = cls.deserialized(**attrs)

        """
        cpy = cls._get_copy_wo_type(kwargs)
        cpy['activity_id'] = cpy.pop('id_', None)

        if cpy.get('retry_strategy'):
            rs = floto.specs.retry_strategy.Strategy.deserialized(**cpy['retry_strategy'])
            cpy['retry_strategy'] = rs

        return cls(**cpy)
