from floto.specs.task import ActivityTask


class Generator(ActivityTask):
    def __init__(self, *, domain, name, version, activity_id=None, requires=None, input=None,
                 retry_strategy=None):
        """Defines an activity task which generates <floto.specs.Task> objects. The generated tasks
        are added to the execution logic of the decider.
        """
        super().__init__(domain=domain, name=name, version=version, activity_id=activity_id, requires=requires,
                         input=input, retry_strategy=retry_strategy)

    @classmethod
    def deserialized(cls, **kwargs):
        """Construct an instance from a dict of attributes

        Notes
        -----
        Note that the parameter `activity_id` is assigned to the attribute `id_ `
        """
        kwargs['activity_id'] = kwargs.pop('id_', None)
        return cls(**kwargs)
