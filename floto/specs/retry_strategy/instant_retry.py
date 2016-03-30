from floto.specs.retry_strategy import Strategy

class InstantRetry(Strategy):
    def __init__(self, retries=None):
        self.retries = retries

    def is_task_resubmitted(self, failures):
        return failures <= self.retries

    @classmethod
    def deserialized(cls, **kwargs):
        cpy = {k:v for k,v in kwargs.items() if not 'type' in k}
        obj = None
        try:
            obj = cls(**cpy)
        except TypeError:
            msg = 'Can not create instance of {} with arguments {}'.format(cls.__name__, kwargs)
            logger.error(msg)
            raise TypeError(msg)
        return obj

