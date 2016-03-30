import logging
import hashlib
import json

import floto.specs
import floto.specs.serializer

logger = logging.getLogger(__name__)


class Task:
    """Base class for tasks, e.g. ActivityTask, Timer.

    Parameters
    ----------
    id_: str
        The unique id of the timer task
    requires: Optional[list[<floto.specs.task>]]
        List of other tasks on which this timer depends on when the workflow is executed

    """

    # TODO `requires` can be anything here, make sure it's a list of tasks
    def __init__(self, id_=None, requires=None):
        self.id_ = id_
        self.requires = requires

    def _default_id(self, *, domain, name, version, input):
        """Create a hex digest from name, version, domain, input (to be used as unique id)

        Returns
        -------
        str

        """
        # TODO change
        input_string = json.dumps(input, sort_keys=True) 

        if self.requires:
            requires_string = ''.join([t.id_ for t in self.requires])
        else:
            requires_string = ''

        hash_ = hashlib.sha1((input_string + requires_string).encode()).hexdigest()[:10]
        return '{}:{}:{}:{}'.format(name, version, domain, hash_)

    def serializable(self):
        """Serializable representation of self

        Returns
        -------
        dict
        """
        class_path = floto.specs.serializer.class_path(self)
        cpy = floto.specs.serializer.serializable(self.__dict__, class_path)

        # in the requires list, we need nothing but the task's id_
        requires = cpy.get('requires')
        if requires:
            cpy['requires'] = []
            for task in requires:
                task_serializable = task.serializable()
                cpy['requires'].append({'id_':task_serializable['id_'], 
                                        'type':task_serializable['type']})

        logger.debug('serialized {}'.format(cpy))
        return cpy

    @staticmethod
    def deserialized(**kwargs):
        if not 'type' in kwargs:
            raise ValueError('No type provided for deserialization')

        cls = floto.specs.serializer.get_class(kwargs['type']) 
        return cls._deserialized(**kwargs)
    
    @classmethod
    def _deserialized(cls, **kwargs):
        cpy = floto.specs.serializer.copy_args_wo_type(kwargs)
        return cls(**cpy)
