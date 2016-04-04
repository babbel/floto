import logging

from floto.specs.task import Task
import floto.specs.retry_strategy

logger = logging.getLogger(__name__)

class ChildWorkflow(Task):
    """ChildWorkflow task spec."""

    def __init__(self, *, domain, workflow_type_name, workflow_type_version, id_=None,
                 requires=None, input=None, task_list=None, retry_strategy=None):
        """Defines a child workflow which is scheduled as part of a decider execution logic and
        used in decider specs.
        
        Parameters
        ----------
        workflow_type_name: str
            The name of the workflow type to be scheduled
        workflow_type_version: str
            The version of the workflow type to be scheduled
        id_: Optional[str]
            The id of the workflow. 
            Defaults to <workflow_type_name>:<workflow_type_version>:hash(input/requires)
        requires: Optional[list]
            List of tasks this child workflow execution depends on
        input: Optional[dict]
            Input provided to this child workflow execution
        task_list: Optional[str]
            The name of the task list to be used for decision tasks of the child workflow 
            execution. If not set, the default task list of the workflow type is used.
        retry_strategy: Optional[floto.specs.Strategy]
            The strategy which defines the repeated execution strategy.
        """
        super().__init__(requires=requires)
        if retry_strategy and not isinstance(retry_strategy, floto.specs.retry_strategy.Strategy):
            raise ValueError('Retry strategy must be of type floto.specs.retry_strategy.Strategy')

        self.domain = domain
        self.workflow_type_name = workflow_type_name
        self.workflow_type_version = workflow_type_version
        self.input = input
        self.task_list = task_list
        self.retry_strategy = retry_strategy

        default_id = self._default_id(domain=domain, name=workflow_type_name,
                                      version=workflow_type_version, input=input)
        self.id_ = id_ or default_id

    def serializable(self):
        cpy = super().serializable()

        retry_strategy = cpy.get('retry_strategy')
        if retry_strategy:
            cpy['retry_strategy'] = retry_strategy.serializable()

        logger.debug('Serialized ChildWorkflow: {}'.format(cpy))
        return cpy

    @classmethod
    def deserialized(cls, **kwargs):
        """Construct an instance from a dict of attributes
        """
        cpy = floto.specs.serializer.copy_dict(kwargs, ['type'])
        if cpy.get('retry_strategy'):
            rs = floto.specs.serializer.get_class(cpy['retry_strategy']['type'])
            cpy['retry_strategy'] = rs.deserialized(**cpy['retry_strategy'])
        return cls(**cpy)
