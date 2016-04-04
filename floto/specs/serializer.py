import logging

from collections import OrderedDict
import sys

logger = logging.getLogger(__name__)

def class_path(obj):
    module_name = '.'.join(obj.__module__.split('.')[:-1])
    return module_name + '.' + obj.__class__.__name__

def ordered_dict_with_type(dict_, class_path, filter_none=True):
    cpy = {k:v for k,v in dict_.items() if (not filter_none) or (v is not None)}
    cpy['type'] = class_path
    return dict_to_ordered_dict(cpy)

def get_class(class_path):
    parts = class_path.split('.')
    module_name = '.'.join(parts[:-1])
    class_name = parts[-1]
    return getattr(sys.modules[module_name], class_name)

def copy_dict(dict_, filter_keys=None):
    """Returns copy of dict_, removes (key, value) if value == None or key is in filter_keys.
    """
    if filter_keys is None:
        filter_keys = []
    new_dict = {k:v for k,v in dict_.items() if (v is not None) and (not k in filter_keys)}
    logger.debug('Copied dict {}:'.format(new_dict))
    return new_dict

def dict_to_ordered_dict(dict_):
    d = OrderedDict.fromkeys(sorted(dict_.keys()))
    for attr, value in sorted(dict_.items()):
        if value is not None:
            d.update({attr: value})
    return d

