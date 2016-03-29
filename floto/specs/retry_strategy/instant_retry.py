from floto.specs.retry_strategy import Strategy


class InstantRetry(Strategy):
    def __init__(self, retries=None):
        self.retries = retries

    def is_task_resubmitted(self, failures):
        return failures <= self.retries

    @classmethod
    def deserialized(cls, **kwargs):
        """Construct an instance from a dict of attributes
        """
        return cls(**kwargs)
