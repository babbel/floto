from floto.specs.task import ActivityTask


class Generator(ActivityTask):
    def __init__(self, *, domain, name, version, id_=None, requires=None, input=None,
                 retry_strategy=None):
        """Defines an activity task which generates <floto.specs.Task> objects. The generated tasks
        are added to the execution logic of the decider.
        """
        super().__init__(domain=domain, name=name, version=version, id_=id_, requires=requires,
                         input=input, retry_strategy=retry_strategy)

