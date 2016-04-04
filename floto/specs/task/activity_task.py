import logging

from floto.specs.task import Task
import floto.specs

logger = logging.getLogger(__name__)


class ActivityTask(Task):
    def __init__(self, *, domain, name, version, id_=None, requires=None, input=None,
                 retry_strategy=None, task_list=None):
        """Defines an activity task which is used in decider specs.

        Parameters
        ----------
        name: str
        version: str
        id_: Optional[str]
            The id of the activity task. Defaults to: <name>:<version>:hash(input/requires)
        requires: Optional[list[str]]
            List of task ids this activity task depends on
        input: Optional[dict]
        retry_strategy: Optional[floto.specs.Strategy]
        task_list : Optional[str]

        """
        super().__init__(requires=requires)

        if retry_strategy and not isinstance(retry_strategy, floto.specs.retry_strategy.Strategy):
            raise ValueError('Retry strategy must be of type floto.specs.retry_strategy.Strategy')

        self.domain = domain
        self.name = name
        self.version = version
        self.input = input
        self.id_ = id_ or self._default_id(domain=domain, name=name, version=version, input=input)
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
    def deserialized(cls, **kwargs):
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
        cpy = floto.specs.serializer.copy_dict(kwargs, ['type'])

        if cpy.get('retry_strategy'):
            rs = floto.specs.serializer.get_class(cpy['retry_strategy']['type'])
            cpy['retry_strategy'] = rs.deserialized(**cpy['retry_strategy'])

        logger.debug('Deserialize ActivityTask with: {}'.format(cpy))
        return cls(**cpy)
