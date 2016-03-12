import hashlib
import json

class Task(object):
    """Base class for tasks, e.g. ActivityTask, Timer."""

    def __init__(self, id_=None, requires=None):
        self.id_ = id_
        self.requires = requires

    def _default_id(self, name, version, input):
        input_string = json.dumps(input, sort_keys=True)
        input_hash = hashlib.sha1(input_string.encode()).hexdigest()[:15]
        return '{}:{}:{}'.format(name, version, input_hash)
