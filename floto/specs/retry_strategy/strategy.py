import logging

import floto.specs.serializer
import floto
import floto.specs

logger = logging.getLogger(__name__)

class Strategy:
    def is_task_resubmitted(self, failures):
        raise NotImplementedError

    def serializable(self):
        class_path = floto.specs.serializer.class_path(self)
        s = floto.specs.serializer.ordered_dict_with_type(self.__dict__, class_path)
        return s

    @classmethod
    def deserialized(cls, **kwargs):
        cpy = floto.specs.serializer.copy_dict(kwargs, filter_keys=['type'])
        return cls(**cpy)




