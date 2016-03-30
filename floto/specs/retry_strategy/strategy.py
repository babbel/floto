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
        s = floto.specs.serializer.serializable(self.__dict__, class_path)
        return s

    @staticmethod
    def deserialized(**kwargs):
        if not 'type' in kwargs:
            raise ValueError('No type provided for deserialization')

        cls = floto.specs.serializer.get_class(kwargs['type']) 
        return cls._deserialized(**kwargs)

    @classmethod
    def _deserialized(cls, **kwargs):
        cpy = floto.specs.serializer.copy_args_wo_type(kwargs)
        return cls(**cpy)




