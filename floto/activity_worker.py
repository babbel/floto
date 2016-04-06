import json
import logging
import socket
import sys
from inspect import signature

import floto.api
import floto.specs

class ActivityWorker:
    """The worker which performs the activities.
    Usage:
    -----
    @floto.activity(domain='my_domain', name='my_activity', version='v1')
    def activity1(context):
        # Do work
        return {'my':'result'}
    
    my_activity_worker = ActivityWorker(task_list='my_tl', domain='my_domain')
    my_activity_worker.run()
    """

    def __init__(self, *, domain, task_list, swf=None, task_heartbeat_in_seconds=None,
            identity=None):
        """
        Parameters
        ----------
        domain: str
        task_list: str
            The task_list of the activity worker
        swf: Optional[floto.api.Swf]
            If None a new instance is initiated
        task_heartbeat_in_seconds: Optional[int]
            Heartbeats are sent every <task_heartbeat_in_seconds> to SWF during the execution. If
            set to 0 no heartbeats will be sent. Default is 120.
        identity: Optional[str]
            Identity of the worker making the request, recorded in the ActivityTaskStarted event in
            the workflow history. This enables diagnostic tracing when problems arise. The form of
            this identity is user defined. Default is the fully qualified domain name.
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
        if self.task_heartbeat_in_seconds is None:
            self.task_heartbeat_in_seconds = 90

        self.identity = identity
        if self.identity is None:
            self.identity = socket.getfqdn(socket.gethostname())

        self.heartbeat_sender = floto.HeartbeatSender()
        self.logger = floto.Logger.getLogger(__name__, self.identity)

    def poll(self):
        self.logger.debug('ActivityWorker.poll...')
        self.last_response = self.swf.poll_for_activity_task(domain=self.domain,
                                                             task_list=self.task_list,
                                                             identity=self.identity)
        if 'taskToken' in self.last_response:
            self.task_token = self.last_response['taskToken']
        else:
            self.task_token = None

    def run(self):
        self.logger.debug('ACTIVITY_FUNCS: {}'.format(floto.ACTIVITY_FUNCTIONS))
        number_polls = 0
        while (not self.get_terminate_activity_worker()) and (number_polls < self.max_polls):
            self.poll()
            number_polls += 1
            if self.task_token:
                activity_type_name = self.last_response['activityType']['name']
                activity_type_version = self.last_response['activityType']['version']
                function_id = activity_type_name + ':' + activity_type_version + ':' + self.domain
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
                            self.logger.warning(e)
                    else:
                        msg = 'No activity with id {} registered'.format(function_id)
                        self.logger.error(msg)
                        raise ValueError(msg)
                except Exception as e:
                    self.stop_heartbeat()
                    try:
                        self.task_failed(e)
                    except Exception as e2:
                        self.logger.warning(e)

    def get_context(self):
        context = {}
        if 'input' in self.last_response:
            context = floto.specs.JSONEncoder.load_string(self.last_response['input'])
        return context

    def get_terminate_activity_worker(self):
        return self._terminate_activity_worker

    def task_failed(self, error):
        self.logger.debug('ActivityWorker.task_failed...')
        self.swf.client.respond_activity_task_failed(taskToken=self.task_token, details=str(error))

    def terminate_worker(self):
        self._terminate_activity_worker = True

    def complete(self):
        self.logger.debug('ActivityWorker.complete...')
        args = {'taskToken': self.task_token}
        if self.result:
            args['result'] = floto.specs.JSONEncoder.dump_object(self.result)
        self.swf.client.respond_activity_task_completed(**args)

    def start_heartbeat(self):
        self.logger.debug('ActivityWorker.start_heartbeat...')
        args = {'timeout': self.task_heartbeat_in_seconds,
                'task_token': self.task_token}
        if self.task_heartbeat_in_seconds:
            self.heartbeat_sender.send_heartbeats(**args)

    def stop_heartbeat(self):
        self.logger.debug('ActivityWorker.start_heartbeat...')
        self.heartbeat_sender.stop_heartbeats()

