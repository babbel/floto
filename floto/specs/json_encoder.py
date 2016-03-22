import collections
import datetime as dt
import json
import sys

import floto
import floto.specs
import floto.specs.retry_strategy


class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (floto.specs.ActivityTask,
                            floto.specs.DeciderSpec,
                            floto.specs.retry_strategy.Strategy,
                            floto.specs.Timer,
                            floto.specs.ChildWorkflow)):
            return self.default_from_namespace(obj)

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
    def object_hook(dct):
        if 'type' in dct:
            return floto.specs.JSONEncoder.deserialize_object(dct)
        else:
            return dct

    @staticmethod
    def deserialize_object(dct):
        class_path = dct['type'].split('.')
        module_name = '.'.join(class_path[:-1])
        class_name = class_path[-1]
        obj = getattr(sys.modules[module_name], class_name)()
        floto.specs.JSONEncoder.update_namespace(obj, dct)
        return obj

    @staticmethod
    def update_namespace(obj, new_dict):
        obj.__dict__ = floto.specs.JSONEncoder.update_dict(obj.__dict__, new_dict)

    @staticmethod
    def update_dict(old_dict, update):
        for k, v in update.items():
            if isinstance(v, collections.Mapping):
                old_dict[k] = floto.specs.JSONEncoder.update_dict(old_dict.get(k, {}), v)
            else:
                if not old_dict:
                    old_dict = {}
                old_dict[k] = update[k]
        return old_dict

    @staticmethod
    def load_string(json_string):
        """Deserialize `json_string` to Python Object. If `json_string` is not a valid json
        document, just return `json_string`.

        .. warning:: This is extremely forgiving. TODO: rethink.

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
