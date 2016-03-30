import logging

from collections import OrderedDict
import sys

logger = logging.getLogger(__name__)

def class_path(obj):
    module_name = '.'.join(obj.__module__.split('.')[:-1])
    return module_name + '.' + obj.__class__.__name__

def serializable(object_namespace, class_path):
    cpy = dict_to_ordered_dict(object_namespace)
    cpy['type'] = class_path
    return cpy

def get_class(class_path):
    parts = class_path.split('.')
    module_name = '.'.join(parts[:-1])
    class_name = parts[-1]
    return getattr(sys.modules[module_name], class_name)

def copy_args_wo_type(kwargs):
    kwargs = {k:v for k,v in kwargs.items() if not 'type' in k}
    logger.debug('Copied kwargs {}:'.format(kwargs))
    return kwargs

def dict_to_ordered_dict(dict_):
    d = OrderedDict.fromkeys(sorted(dict_.keys()))
    for attr, value in sorted(dict_.items()):
        if value:
            d.update({attr: value})
    return d

