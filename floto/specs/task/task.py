import hashlib
import logging

import floto.specs

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
        input_string = floto.specs.JSONEncoder.dump_object(input)

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
        # remove None values
        cpy = {k: v for k, v in self.__dict__.items() if v is not None}

        # add type
        module_name = '.'.join(self.__module__.split('.')[:-1])
        cpy['type'] = module_name + '.' + self.__class__.__name__

        # in the requires list, we need nothing but the task's id_
        requires = cpy.get('requires')
        if requires:
            cpy['requires'] = [{'id_': t.id_} for t in requires]
        logger.debug('serialized {}'.format(cpy))
        return cpy
