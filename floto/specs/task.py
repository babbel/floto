class Task(object):
    """Base class for tasks, e.g. ActivityTask, Timer."""

    def __init__(self, id_=None, requires=None):
        self.id_ = id_
        self.requires = requires
