import json
import logging
import sys
from inspect import signature

import floto.api
import floto.specs

logger = logging.getLogger(__name__)


class ActivityWorker:
    """The worker which performs the activities.
    Usage:
    -----
    @floto.activity(name='my_activity', version='v1')
    def activity1(context):
        # Do work
        return {'my':'result'}
    
    my_activity_worker = ActivityWorker(task_list='my_tl', domain='my_domain')
    my_activity_worker.run()
    """

    def __init__(self, swf=None, task_list=None, domain=None, task_heartbeat_in_seconds=None):
        """
        Parameters
        ----------
        swf: floto.api.Swf()
            If None a new instance is initiated
        task_list: str [Required]
            The task_list of the activity worker
        domain: str [Required]
        task_heartbeat_in_seconds: int
            Heartbeats are sent every <task_heartbeat_in_seconds> to SWF during the execution. If
            set to 0 no heartbeats will be sent. Default is 120.
        """
        self.task_token = None
        self.last_response = None
        self._terminate_activity_worker = False
        self.max_polls = sys.maxsize
        self.input = None
        self.result = None
        self.swf = swf or floto.api.Swf()
        self.task_list = task_list
        self.domain = domain
        self.task_heartbeat_in_seconds = task_heartbeat_in_seconds
        if self.task_heartbeat_in_seconds == None:
            self.task_heartbeat_in_seconds = 120
        self.heartbeat_sender = floto.HeartbeatSender()

    def poll(self):
        self.last_response = self.swf.poll_for_activity_task(domain=self.domain,
                                                             task_list=self.task_list)
        if 'taskToken' in self.last_response:
            self.task_token = self.last_response['taskToken']
        else:
            self.task_token = None

    def run(self):
        number_polls = 0
        while (not self.get_terminate_activity_worker()) and (number_polls < self.max_polls):
            self.poll()
            number_polls += 1
            if self.task_token:
                activity_type_name = self.last_response['activityType']['name']
                activity_type_version = self.last_response['activityType']['version']
                function_id = activity_type_name + ':' + activity_type_version
                context = self.get_context()
                try:
                    if function_id in floto.ACTIVITY_FUNCTIONS:
                        self.start_heartbeat()
                        activity_function = floto.ACTIVITY_FUNCTIONS[function_id]
                        if 'context' in signature(activity_function).parameters:
                            self.result = activity_function(context=context)
                        else:
                            self.result = activity_function()
                        self.stop_heartbeat()
                        try:
                            self.complete()
                        except Exception as e:
                            logger.warning(e)
                    else:
                        raise ValueError('No activity with id {} registered'.format(function_id))
                except Exception as e:
                    self.stop_heartbeat()
                    try:
                        self.task_failed(e)
                    except Exception as e2:
                        logger.warning(e)

    def get_context(self):
        context = {}
        if 'input' in self.last_response:
            context = floto.specs.JSONEncoder.load_string(self.last_response['input'])
        return context

    def get_terminate_activity_worker(self):
        return self._terminate_activity_worker

    def task_failed(self, error):
        self.swf.client.respond_activity_task_failed(taskToken=self.task_token, details=str(error))

    def terminate_worker(self):
        self._terminate_activity_worker = True

    def complete(self):
        args = {'taskToken': self.task_token}
        if self.result and isinstance(self.result, str):
            args['result'] = self.result
        if self.result and isinstance(self.result, dict):
            args['result'] = json.dumps(self.result)
        self.swf.client.respond_activity_task_completed(**args)

    def start_heartbeat(self):
        args = {'timeout': self.task_heartbeat_in_seconds,
                'task_token': self.task_token}
        if self.task_heartbeat_in_seconds:
            self.heartbeat_sender.send_heartbeats(**args)

    def stop_heartbeat(self):
        self.heartbeat_sender.stop_heartbeats()
