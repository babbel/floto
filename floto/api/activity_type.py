import logging

from floto.api import SwfType

logger = logging.getLogger(__name__)


class ActivityType(SwfType):
    """
    Attributes
    ----------
    default_task_heartbeat_timeout : Optional[str]
        Specify the default maximum time before which a worker processing a task of this type
        must report progress by calling RecordActivityTaskHeartbeat . If the timeout is exceeded,
        the activity task is automatically timed out.
    default_task_schedule_to_start_timeout : Optional[str]
    default_task_schedule_to_close_timeout : Optional[str]
    """

    def __init__(self, *, domain, name, version,
                 description=None,
                 default_task_list='default',
                 default_task_start_to_close_timeout=None,
                 default_task_priority='0',
                 default_task_heartbeat_timeout=None,
                 default_task_schedule_to_start_timeout=None,
                 default_task_schedule_to_close_timeout=None):

        if default_task_start_to_close_timeout is None:
            default_task_start_to_close_timeout = str(60 * 60 * 6)

        super().__init__(domain=domain, name=name, version=version,
                         description=description,
                         default_task_list=default_task_list,
                         default_task_start_to_close_timeout=default_task_start_to_close_timeout,
                         default_task_priority=default_task_priority)

        self.default_task_heartbeat_timeout = default_task_heartbeat_timeout or default_task_start_to_close_timeout
        self.default_task_schedule_to_start_timeout = default_task_schedule_to_start_timeout or str(60 * 60 * 1)
        self.default_task_schedule_to_close_timeout = default_task_schedule_to_close_timeout or str(
            60 * 60 * 1 + int(self.default_task_start_to_close_timeout))

    @property
    def swf_attributes(self):
        """Class attributes as wanted by the AWS SWF API """
        a = {'domain': self.domain,
             'name': self.name,
             'version': self.version}
        if self.description is not None:
            a['description'] = self.description
        if self.default_task_list is not None:
            a['defaultTaskList'] = {'name': self.default_task_list}
        if self.default_task_start_to_close_timeout is not None:
            a['defaultTaskStartToCloseTimeout'] = self.default_task_start_to_close_timeout
        if self.default_task_priority is not None:
            a['defaultTaskPriority'] = self.default_task_priority

        if self.default_task_heartbeat_timeout is not None:
            a['defaultTaskHeartbeatTimeout'] = self.default_task_heartbeat_timeout
        if self.default_task_schedule_to_start_timeout is not None:
            a['defaultTaskScheduleToStartTimeout'] = self.default_task_schedule_to_start_timeout
        if self.default_task_schedule_to_close_timeout is not None:
            a['defaultTaskScheduleToCloseTimeout'] = self.default_task_schedule_to_close_timeout

        return a
