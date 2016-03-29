import collections
import datetime as dt
import json
import logging
import sys

import floto
import floto.specs
import floto.specs.retry_strategy
import floto.specs.task

logger = logging.getLogger(__name__)


class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (floto.specs.DeciderSpec,
                            floto.specs.retry_strategy.Strategy)):
            return self.default_from_namespace(obj)

        if isinstance(obj, (floto.specs.task.ActivityTask,
                            floto.specs.task.Timer,
                            floto.specs.task.ChildWorkflow,
                            floto.specs.task.Generator)):
            return obj.serializable()

        if isinstance(obj, (dt.datetime,
                            dt.date,
                            dt.time)):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)

    def default_from_namespace(self, obj):
        d = self.filter_none(obj.__dict__)
        module_name = '.'.join(obj.__module__.split('.')[:-1])
        d['type'] = module_name + '.' + obj.__class__.__name__
        return d

    @staticmethod
    def filter_none(dictionary):
        return {k: v for k, v in dictionary.items() if v is not None}

    @staticmethod
    def floto_object_hook(dct):
        if 'type' in dct:
            return floto.specs.JSONEncoder.deserialize_object(dct)
        else:
            return dct

    @staticmethod
    def deserialize_object(dct):
        """Construct a floto object from a dictionary
        Notes
        -----

        The class needs a `deserialized` constructor or an AttributeError will be raised
        """
        attrs = floto.specs.JSONEncoder.update_dict({}, dct)
        attrs.pop('type', None)

        class_path = dct['type'].split('.')
        module_name = '.'.join(class_path[:-1])
        class_name = class_path[-1]
        cls = getattr(sys.modules[module_name], class_name)
        try:
            obj = cls.deserialized(**attrs)
        except AttributeError as e:
            logger.error('Missing deserialized constructor: {}'.format(e))
            raise e

        return obj

    @staticmethod
    def update_namespace(obj, new_dict):
        new_attrs = floto.specs.JSONEncoder.update_dict(obj.__dict__, new_dict)
        print(new_attrs)
        for key, value in new_attrs.items():
            if key != type:
                setattr(obj, key, value)

    @staticmethod
    def update_dict(old_dict, update):
        logger.debug('Update dict...{}'.format(update))
        for k, v in update.items():
            if isinstance(v, collections.Mapping):
                logger.debug('update dict Mapping {} - to {}'.format(old_dict.get(k, {}), v))
                old_dict[k] = floto.specs.JSONEncoder.update_dict(old_dict.get(k, {}), v)
            else:
                if not old_dict:
                    old_dict = {}
                old_dict[k] = update[k]
        return old_dict

    @staticmethod
    def load_string(json_string):
        """Deserialize `json_string` to Python Object. If `json_string` is not a valid json
        document, just return `json_string`. It is used in the context of activity results: floto
        handles str and JSON serialized results.

        Parameters
        ----------
        json_string : any

        Returns
        -------

        obj

        """
        try:
            j = json.loads(json_string)
        except (TypeError, json.JSONDecodeError):
            j = json_string
        return j

    @staticmethod
    def dump_object(obj):
        if isinstance(obj, str):
            j = obj
        else:
            j = json.dumps(obj, cls=floto.specs.JSONEncoder, sort_keys=True)
        return j
