import logging
import sys
from collections import OrderedDict


logger = logging.getLogger(__name__)

class JSONSerializable:
    def serializable(self):
        """Serializable representation of self

        Returns
        -------
        dict
        """
        cpy = self._to_ordered_dict() 

        # add type
        module_name = '.'.join(self.__module__.split('.')[:-1])
        cpy['type'] = module_name + '.' + self.__class__.__name__
        return cpy


    @staticmethod
    def deserialized(**kwargs):
        """Construct an instance from a dict of attributes with type information.
        """
        if not 'type' in kwargs:
            raise ValueError('No type provided for deserialization')

        cls = JSONSerializable._get_class(kwargs['type']) 
        return cls._deserialized(**kwargs)

    @classmethod
    def _deserialized(cls, **kwargs):
        """Standard implementation of the deserialization.
        """
        cpy = cls._get_copy_wo_type(kwargs)
        return cls._instantiate(**cpy)

    @classmethod
    def _instantiate(cls, **kwargs):
        obj = None
        try:
            obj = cls(**kwargs)
        except TypeError:
            msg = 'Can not create instance of {} with arguments {}'.format(cls.__name__, kwargs)
            logger.error(msg)
            raise TypeError(msg)
        return obj

    @classmethod
    def _get_copy_wo_type(cls, kwargs):
        return {k:v for k,v in kwargs.items() if not 'type' in k}

    # TODO test
    @staticmethod
    def _get_class(type_):
        class_path = type_.split('.')
        module_name = '.'.join(class_path[:-1])
        class_name = class_path[-1]
        return getattr(sys.modules[module_name], class_name)

    # TODO test
    def _to_ordered_dict(self):
        """Return attributes as OrderedDict
        Returns
        -------
        OrderedDict
        """

        d = OrderedDict.fromkeys(sorted(self.__dict__.keys()))
        for attr, value in sorted(self.__dict__.items()):
            if value:
                d.update({attr: value})
        return d
