import logging

from floto.specs import JSONSerializable
import floto.specs

logger = logging.getLogger(__name__)

class Strategy(JSONSerializable):
    def is_task_resubmitted(self, failures):
        raise NotImplementedError

