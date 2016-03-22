import hashlib
import json

class Task(object):
    """Base class for tasks, e.g. ActivityTask, Timer."""

    def __init__(self, id_=None, requires=None):
        self.id_ = id_
        self.requires = requires

    def _default_id(self, name, version, input):
        """Create a hexdigest from name, version, input (to be used as unique id)

        Returns
        -------
        str

        """
        input_string = json.dumps(input, sort_keys=True)

        if self.requires:
            requires_string = ''.join([t.id_ for t in self.requires])
        else:
            requires_string = ''

        hash_ = hashlib.sha1((input_string+requires_string).encode()).hexdigest()[:10]
        return '{}:{}:{}'.format(name, version, hash_)
