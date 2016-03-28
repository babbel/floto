from floto.specs.task import Task


class Timer(Task):
    """Timer task spec."""

    def __init__(self, id_=None, requires=None, delay_in_seconds=None):
        """
        Parameter
        --------
        id_: str [Required]
            The unique id of the timer task
        requires: list
            List of other tasks on which this timer depends on when the workflow is executed
        delay_in_seconds: int
        """
        super().__init__(id_=id_, requires=requires)
        self.delay_in_seconds = delay_in_seconds

    @classmethod
    def deserialized(cls, **kwargs):
        """Construct an instance from a dict of attributes
        """
        return cls(**kwargs)
