import logging
import hashlib
from collections import OrderedDict
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
    requires: Optional[list[str]]
        List of other task ids on which this task depends on when the workflow is executed

    """

    def __init__(self, id_=None, requires=None):
        self.id_ = id_

        if requires and not all([isinstance(t, str) for t in requires]):
            raise ValueError('requires must be list of str')

        self.requires = requires

    def _default_id(self, *, domain, name, version, input):
        """Create a hex digest from name, version, domain, input (to be used as unique id)

        Returns
        -------
        str

        """
        input_string = json.dumps(input, sort_keys=True) 

        if self.requires:
            requires_string = ''.join( self.requires)
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
        cpy = floto.specs.serializer.ordered_dict_with_type(self.__dict__, class_path)
        logger.debug('serialized {}'.format(cpy))
        return cpy

    @classmethod
    def deserialized(cls, **kwargs):
        cpy = floto.specs.serializer.copy_dict(kwargs, filter_keys=['type'])
        return cls(**cpy)

