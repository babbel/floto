import json
import logging

from floto.decisions import Decision

logger = logging.getLogger(__name__)

class ScheduleActivityTask(Decision):
    def __init__(self, **args):
        self.activity_type = args.get('activity_type', None)
        self.activity_id = args.get('activity_id', None)
        self.input = args.get('input', None)
        self.task_list = args.get('task_list', None)

        self.required_fields = ['decisionType',
                                'scheduleActivityTaskDecisionAttributes.activityType.name',
                                'scheduleActivityTaskDecisionAttributes.activityType.version']

    def _get_decision(self):
        d = {'decisionType': 'ScheduleActivityTask',
             'scheduleActivityTaskDecisionAttributes': self.decision_attributes()
             }
        return d

    def decision_attributes(self):
        logger.debug('ScheduleActivityTask.decision_attributes...')
        if not self.activity_type:
            raise ValueError('No activity type')

        activity_id = self.activity_id or self.activity_type.name + '_' + \
                                          self.activity_type.version

        attributes = {'activityType': {
            'name': self.activity_type.name,
            'version': self.activity_type.version
        },
            'activityId': activity_id}

        if self.task_list:
            attributes['taskList'] = {'name': self.task_list}

        if self.input:
            attributes['input'] = json.dumps(self.input)

        return attributes
