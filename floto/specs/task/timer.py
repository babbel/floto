from floto.specs.task import Task


class Timer(Task):
    """Timer task spec."""

    def __init__(self, *, id_, requires=None, delay_in_seconds=None):
        """
        Parameters
        ----------
        id_: str
            The unique id of the timer task
        requires: Optional[list[<floto.specs.task>]]
            List of other tasks on which this timer depends on when the workflow is executed
        delay_in_seconds: int
        """
        super().__init__(id_=id_, requires=requires)
        self.delay_in_seconds = delay_in_seconds

