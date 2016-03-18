from floto.specs import ActivityTask

class Generator(ActivityTask):
    def __init__(self, name=None, version=None, activity_id=None, requires=None, input=None,
                 retry_strategy=None):
        """Defines an activity task which generates <floto.specs.Task> objects. The generated tasks
        are added to the execution logics of the decider.
        """
        super().__init__(name=name, version=version, activity_id=activity_id, requires=requires, 
                input=input, retry_strategy=retry_strategy)


