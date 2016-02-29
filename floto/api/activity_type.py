import logging

from floto.api import SwfType

logger = logging.getLogger(__name__)


class ActivityType(SwfType):
    def __init__(self, domain=None, name=None, version=None, **args):
        super().__init__(domain=domain, name=name, version=version, **args)

        activity_type_attributes = ['defaultTaskHeartbeatTimeout',
                                    'defaultTaskScheduleToStartTimeout',
                                    'defaultTaskScheduleToCloseTimeout']

        default_values = {'defaultTaskHeartbeatTimeout': self.default_task_start_to_close_timeout,
                          'defaultTaskScheduleToStartTimeout': str(60 * 60 * 1),
                          'defaultTaskScheduleToCloseTimeout': str(
                              60 * 60 * 1 + int(self.default_task_start_to_close_timeout))}

        self._set_attributes(activity_type_attributes, args, default_values)
        self.attributes += activity_type_attributes

    @property
    def default_task_heartbeat_timeout(self):
        """Specify the default maximum time before which a worker processing a task of this type
        must report progress by calling RecordActivityTaskHeartbeat . If the timeout is exceeded, 
        the activity task is automatically timed out.
        """
        return self._default_task_heartbeat_timeout

    @property
    def default_task_schedule_to_start_timeout(self):
        return self._default_task_schedule_to_start_timeout

    @property
    def default_task_schedule_to_close_timeout(self):
        return self._default_task_schedule_to_close_timeout

    @default_task_heartbeat_timeout.setter
    def default_task_heartbeat_timeout(self, timeout):
        self._default_task_heartbeat_timeout = timeout

    @default_task_schedule_to_start_timeout.setter
    def default_task_schedule_to_start_timeout(self, timeout):
        self._default_task_schedule_to_start_timeout = timeout
