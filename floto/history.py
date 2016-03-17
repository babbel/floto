import floto.api

class History:
    """History provides information based on the events recorded by SWF during the workflow
    execution"""

    def __init__(self, domain, task_list, response):
        """
        Parameters
        ----------
        domain: str
        task_list: str
            Task list of the decider
        response: dict
            The response from 'poll_for_decision_task'
        """
        self.domain = domain
        self.task_list = task_list

        self.events_by_type = {}
        self.events_by_id = {}
        self.events_by_activity_id = {}

        self.set_response_properties(response)
        self.dt_previous_decision_task = None
        self._read_events_up_to_last_decision(response)

    def set_response_properties(self, response):
        self.next_page_token = response['nextPageToken'] if ('nextPageToken' in response) else None
        self.highest_event_id = response['events'][0]['eventId']
        self.lowest_event_id = response['events'][-1]['eventId']
        self.decision_task_started_event_id = response['startedEventId']
        self.previous_decision_id = response['previousStartedEventId']

    def get_event(self, event_id=None, allow_read_next_event_page=True):
        if not event_id:
            raise ValueError("event_id is mandatory")
        if not (0 < event_id <= self.highest_event_id):
            raise ValueError("Event with id: {} does not exist".format(event_id))
        e = None
        if event_id in self.events_by_id:
            e = self.events_by_id[event_id]
        elif event_id < self.lowest_event_id and allow_read_next_event_page:
            self._read_next_event_page()
            e = self.get_event(event_id, allow_read_next_event_page)
        return e

    def get_events_by_type(self, event_type):
        return self.events_by_type[event_type] if event_type in self.events_by_type else []

    def get_events_up_to_last_decision(self, event_type):
        return self.get_events_up_to_datetime(event_type, self.dt_previous_decision_task)

    def get_events_up_to_datetime(self, event_type, dt):
        events = []
        if event_type in self.events_by_type:
            events = [e for e in self.events_by_type[event_type] if e['eventTimestamp'] >= dt]
        return events

    def get_events_by_task_id_and_type(self, id_, event_type):
        """Returns the events by the id of the activity task or timer
        Returns
        -------
        list: dict conforming to <event_type>
            If no events for id_ have been recorded an empty list is returned
        """
        if id_ in self.events_by_activity_id and \
                        event_type in self.events_by_activity_id[id_]:
            return self.events_by_activity_id[id_][event_type]
        return []

    def get_events_for_decision(self, first_event_id, last_event_id):
        """Returns events which influence the following decisions by the DecisionBuilder.
        Parameter
        ---------
        first_event_id: int
           The smallest event id of the range of which the events are retrieved
        last_event_id: int
           The biggest event id of the range of which the events are retrieved

        Returns
        -------
        dict: 
            keys: (faulty, completed, decision_failed)
        """
        types_faulty = ['ActivityTaskFailed', 
                'ActivityTaskTimedOut',
                'ChildWorkflowExecutionFailed',
                'ChildWorkflowExecutionTimedOut',
                'ChildWorkflowExecutionCanceled',
                'ChildWorkflowExecutionTerminated']

        types_completed = ['ActivityTaskCompleted', 
                'TimerFired',
                'ChildWorkflowExecutionCompleted']

        types_decision_failed = ['DecisionTaskTimedOut']
        events = [self.get_event(i) for i in range(first_event_id, last_event_id + 1)]
        decider_events = {}
        decider_events['faulty'] = self._filter_events_by_type(events, types_faulty)
        decider_events['completed'] = self._filter_events_by_type(events, types_completed)
        decision_failed_events = self._filter_events_by_type(events, types_decision_failed)
        decider_events['decision_failed'] = decision_failed_events
        return decider_events

    def get_event_by_task_id_and_type(self, id_, type_):
        """Returns the latest event of type <type_> for id_.
        Returns None if no matching event is found."""
        events = self.get_events_by_task_id_and_type(id_, type_)
        if events:
            return events[0]
        elif self._has_next_event_page():
            self._read_next_event_page()
            return self.get_event_by_task_id_and_type(id_, type_)
        return None
        
    def get_event_attributes(self, event):
        """The attributes of an event."""
        event_type = event['eventType']
        attributes = event_type[:1].lower() + event_type[1:] + 'EventAttributes'
        return event[attributes]

    def get_id_task_event(self, event, allow_read_next_event_page=True):
        """The activity_id or timer_id corresponding to an event."""
        types = {'ActivityTaskFailed': self.get_id_activity_task_event,
                 'ActivityTaskTimedOut': self.get_id_activity_task_event,
                 'ActivityTaskCompleted': self.get_id_activity_task_event,
                 'ActivityTaskScheduled': self.get_id_activity_task_scheduled,
                 'TimerStarted': self.get_id_timer_fired_event,
                 'TimerFired': self.get_id_timer_fired_event,
                 'ChildWorkflowExecutionCompleted': self.get_id_child_workflow_event,
                 'ChildWorkflowExecutionFailed': self.get_id_child_workflow_event,
                 'ChildWorkflowExecutionTimedOut': self.get_id_child_workflow_event,
                 'ChildWorkflowExecutionCanceled': self.get_id_child_workflow_event,
                 'ChildWorkflowExecutionTerminated': self.get_id_child_workflow_event,
                 'StartChildWorkflowExecutionInitiated': 
                     self.get_id_start_child_workflow_execution_initiated}

        if not event['eventType'] in types:
            raise ValueError('Do not know how to retrieve id of {}'.format(event['eventType']))

        id_ = types[event['eventType']](event, allow_read_next_event_page)
        return id_

    def get_id_previous_started(self, decision_task_started_event):
        """Returns the id of the DecisionTaskStarted event previous to 
        <decision_task_started_event>."""
        for started in self.get_events_by_type('DecisionTaskStarted'):
            if started['eventId'] < decision_task_started_event['eventId']:
                return started['eventId']
        if self._has_next_event_page():
            self._read_next_event_page()
            return self.get_id_previous_started(decision_task_started_event)
        else:
            return 0

    def get_id_timer_fired_event(self, event, allow_read_next_event_page=True):
        attributes = self.get_event_attributes(event)
        id_ = attributes['timerId']
        return id_

    def get_id_activity_task_event(self, event, allow_read_next_event_page=True):
        attributes = self.get_event_attributes(event)
        scheduled_event_id = attributes['scheduledEventId']
        scheduled_event = self.get_event(scheduled_event_id, allow_read_next_event_page)

        if scheduled_event:
            return scheduled_event['activityTaskScheduledEventAttributes']['activityId']
        else:
            return None

    def get_id_activity_task_scheduled(self, event, allow_read_next_event_page=True):
        return event['activityTaskScheduledEventAttributes']['activityId']

    def get_id_start_child_workflow_execution_initiated(self, event, allow_read_next_page=True):
        attributes = self.get_event_attributes(event)
        id_ = attributes['workflowId']
        return id_

    def get_id_child_workflow_event(self, event, allow_read_next_page=True):
        attributes = self.get_event_attributes(event)
        id_ = attributes['workflowExecution']['workflowId']
        return id_

    def get_number_activity_failures(self, task):
        if isinstance(task, floto.specs.ActivityTask):
            return self.get_number_activity_task_failures(task.id_)
        elif isinstance(task, floto.specs.ChildWorkflow):
            return self.get_number_child_workflow_failures(task.id_)
        else:
            return 0

    def get_number_activity_task_failures(self, activity_id):
        """Number of failed executions of activity task"""
        dt = self.get_datetime_last_event_of_activity(activity_id, 'ActivityTaskCompleted')

        failed = self.get_events_by_task_id_and_type(activity_id, 'ActivityTaskFailed')
        timed_out = self.get_events_by_task_id_and_type(activity_id, 'ActivityTaskTimedOut')

        failed_since_completion = len([e for e in failed if e['eventTimestamp'] > dt])
        timed_out_since_completion = len([e for e in timed_out if e['eventTimestamp'] > dt])

        return failed_since_completion + timed_out_since_completion

    def get_number_child_workflow_failures(self, id_):
        dt = self.get_datetime_last_event_of_activity(id_, 'ChildWorkflowExecutionCompleted')
        failed_event_types = ['ChildWorkflowExecutionFailed',
                'ChildWorkflowExecutionTimedOut',
                'ChildWorkflowExecutionCanceled',
                'ChildWorkflowExecutionTerminated']
        failed_events = [self.get_events_by_task_id_and_type(id_, t) for t in failed_event_types]
        failed_events = [e for events in failed_events for e in events]
        current_failures = [e for e in failed_events if e['eventTimestamp']>dt]
        return len(current_failures)

    def get_datetime_previous_decision(self):
        """The datetime of the previous decision. If there has not been a previous decision task,
        the datetime of the workflow start is returned."""
        dt = None
        if self.previous_decision_id:
            if self.lowest_event_id <= self.previous_decision_id <= self.highest_event_id:
                dt = self.events_by_id[self.previous_decision_id]['eventTimestamp']
            elif self._has_next_event_page():
                self._read_next_event_page()
                dt = self.get_datetime_previous_decision()
        else:
            dt = self.get_event(1)['eventTimestamp']
        return dt

    def get_datetime_last_event_of_activity(self, activity_id, type_):
        """Datetime of latest event of type <type_> with <activity_id>. If not found, the datetime 
        of the workflow start is returned.
        Parameters
        ----------
        activity_id: str
        type_: str
            Type of the activity, e.g. 'ActivityTaskCompleted', 'ChildWorkflowExecutionCompleted'
        """
        events = self.get_events_by_task_id_and_type(activity_id, type_)
        if events:
            return events[0]['eventTimestamp']
        elif self._has_next_event_page():
            self._read_next_event_page()
            return self.get_datetime_last_event_of_activity(activity_id, type_)
        return self.get_event(1)['eventTimestamp']

    def get_workflow_input(self):
        """Returns workflow input

        Returns
        -------
        dict: Workflow input, {} if no input is given
        """
        input = {}
        workflow_start = self.get_events_by_type('WorkflowExecutionStarted')
        if workflow_start:
            attributes = self.get_event_attributes(workflow_start[0])

            if 'input' in attributes:
                input = floto.specs.JSONEncoder.load_string(attributes['input'])
        elif self._has_next_event_page():
            self._read_next_event_page()
            input = self.get_workflow_input()
        return input

    def get_result_completed_activity(self, task):
        if isinstance(task, floto.specs.ActivityTask):
            c = self.get_events_by_task_id_and_type(task.id_, 'ActivityTaskCompleted')
        elif isinstance(task, floto.specs.ChildWorkflow):
            c =  self.get_events_by_task_id_and_type(task.id_, 'ChildWorkflowExecutionCompleted')
        else:
            c = None

        if c:
            attributes = self.get_event_attributes(c[0])
            if attributes['result']:
                return floto.specs.JSONEncoder.load_string(attributes['result'])
            else:
                return None
        elif self._has_next_event_page():
            self._read_next_event_page()
            return self.get_result_completed_activity(task)
        return None

    # TODO: Adapt for StartAsNewWorkflow event
    def is_first_decision_task(self):
        """Returns true if this is the first decision task of the workflow"""
        if self.previous_decision_id == 0:
            return True

    def is_task_completed(self, task):
        """
        Parameter
        --------
        task: floto.specs.ActivityTask or floto.specs.Timer
        """
        if isinstance(task, floto.specs.ActivityTask):
            return self.is_activity_task_completed(task.id_)
        elif isinstance(task, floto.specs.Timer):
            return self.is_timer_task_completed(task.id_)
        elif isinstance(task, floto.specs.ChildWorkflow):
            return self.is_child_workflow_completed(task.id_)
        else:
            raise ValueError('Unknown type: {}'.format(task.__class__.__name__))

    def is_timer_task_completed(self, timer_id):
        """Returns whether timer with <timer_id> has completed or not."""
        timer_started = self.get_events_by_task_id_and_type(timer_id, 'TimerStarted')
        if timer_started:
            timer_fired = self.get_events_by_task_id_and_type(timer_id, 'TimerFired')
            if timer_fired:
                if timer_fired[0]['eventTimestamp'] > timer_started[0]['eventTimestamp']:
                    return True
        elif self._has_next_event_page():
            self._read_next_event_page()
            return self.is_timer_task_completed(timer_id)
        return False

    def is_activity_task_completed(self, activity_id):
        """Returns whether activity task with <activity_id> has completed or not."""
        scheduled = self.get_events_by_task_id_and_type(activity_id, 'ActivityTaskScheduled')
        if scheduled:
            completed = self.get_events_by_task_id_and_type(activity_id, 'ActivityTaskCompleted')
            if completed and completed[0]['eventTimestamp'] > scheduled[0]['eventTimestamp']:
                return True
        elif self._has_next_event_page():
            self._read_next_event_page()
            return self.is_activity_task_completed(activity_id)
        return False

    def is_child_workflow_completed(self, workflow_id):
        initiated = self.get_events_by_task_id_and_type(workflow_id, 
                'StartChildWorkflowExecutionInitiated')
        if initiated:
            completed = self.get_events_by_task_id_and_type(workflow_id, 
                    'ChildWorkflowExecutionCompleted')
            if completed and completed[0]['eventTimestamp'] > initiated[0]['eventTimestamp']:
                return True
        elif self._has_next_event_page():
            self._read_next_event_page()
            return self.is_child_workflow_completed(workflow_id)
        return False

    def _read_events_up_to_last_decision(self, response):
        self._read_event_page(response['events'])
        self.dt_previous_decision_task = self.get_datetime_previous_decision()

    def _read_event_page(self, events):
        for event in events:
            self.events_by_id[event['eventId']] = event

            event_type = event['eventType']
            if not event_type in self.events_by_type:
                self.events_by_type[event_type] = [event]
            else:
                self.events_by_type[event_type].append(event)

        self._fill_events_by_activity_id_for_types(max_event_id=events[0]['eventId'])

    def _fill_events_by_activity_id_for_types(self, max_event_id):
        types = ['ActivityTaskCompleted',
                 'ActivityTaskFailed',
                 'ActivityTaskTimedOut',
                 'ActivityTaskScheduled',
                 'TimerStarted',
                 'TimerFired',
                 'StartChildWorkflowExecutionInitiated',
                 'ChildWorkflowExecutionCompleted',
                 'ChildWorkflowExecutionFailed',
                 'ChildWorkflowExecutionTimedOut',
                 'ChildWorkflowExecutionCanceled',
                 'ChildWorkflowExecutionTerminated']
        for t in types:
            events = self._collect_new_events_for_fill_by_activity_id(t, max_event_id)
            self._fill_events_by_activity_id(events)

    def _collect_new_events_for_fill_by_activity_id(self, event_type, max_event_id):
        events = []
        if 'none' in self.events_by_activity_id:
            if event_type in self.events_by_activity_id['none']:
                events = list(self.events_by_activity_id['none'][event_type])
                self.events_by_activity_id['none'][event_type] = []

        new_events = []
        for e in reversed(self.get_events_by_type(event_type)):
            if e['eventId'] <= max_event_id:
                new_events.append(e)
            else:
                break
        new_events.reverse()
        events.extend(new_events)
        return events

    def _fill_events_by_activity_id(self, events):
        for event in events:
            activity_id = self.get_id_task_event(event, False) or 'none'
            if not activity_id in self.events_by_activity_id:
                self.events_by_activity_id[activity_id] = {}

            if not event['eventType'] in self.events_by_activity_id[activity_id]:
                self.events_by_activity_id[activity_id][event['eventType']] = [event]
            else:
                self.events_by_activity_id[activity_id][event['eventType']].append(event)

    def _read_next_event_page(self):
        if not self.next_page_token:
            raise ValueError('floto.History._read_next_event_page(): No page token!')

        swf = floto.api.Swf()
        poll_args = {'domain': self.domain,
                     'task_list': self.task_list,
                     'page_token': self.next_page_token}
        next = swf.poll_for_decision_task_page(**poll_args)

        self._read_event_page(next['events'])
        self.next_page_token = next['nextPageToken'] if ('nextPageToken' in next) else None
        self.lowest_event_id = next['events'][-1]['eventId']

    def _has_next_event_page(self):
        next_page = True
        if not self.next_page_token:
            next_page = False
        if 'WorkflowExecutionContinuedAsNew' in self.events_by_type:
            next_page = False
        return next_page

    def _filter_events_by_type(self, events, types):
        return [e for e in events if e['eventType'] in types]
