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
        if isinstance(obj, (dt.datetime,
                            dt.date,
                            dt.time)):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)

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
