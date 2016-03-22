import floto
import floto.decisions
import floto.specs
import copy
import gzip

import logging
logger = logging.getLogger(__name__)

import json

class DecisionBuilder:
    def __init__(self, activity_tasks, default_activity_task_list):
        self.workflow_fail = False
        self.workflow_complete = False
        self.initial_activity_tasks = activity_tasks
        self.history = None
        self.default_activity_task_list = default_activity_task_list
        self.decision_input = floto.decider.DecisionInput()

        self._execution_graph = None
        self.first_event_id = None
        self.last_event_id = None

        self._decompress_generator_result = False

    def get_decisions(self, history):
        self._execution_graph = None
        self._set_history(history)

        self.workflow_fail = False
        self.workflow_complete = False
     
        self.first_event_id = self.history.previous_decision_id
        self.last_event_id = self.history.decision_task_started_event_id
        decisions = self._collect_decisions()
        return decisions

    @property 
    def execution_graph(self):
        if not self._execution_graph:
            self._build_execution_logic()
        return self._execution_graph

    def _set_history(self, history):
        self.history = history
        self.decision_input.history = history

    def is_terminate_workflow(self):
        return self.workflow_fail or self.workflow_complete

    def _collect_decisions(self):
        logger.debug('DecisionBuilder._collect_decisions({},{})'.format(self.first_event_id, 
            self.last_event_id))

        if self.first_event_id == 0:
            return self.get_decisions_after_workflow_start()

        decisions = []
        events = self.history.get_events_for_decision(self.first_event_id, self.last_event_id)

        if events['faulty']:
            decisions.extend(self.get_decisions_faulty_tasks(events['faulty']))

        if not self.is_terminate_workflow() and events['completed'] and \
                self.all_workflow_tasks_finished(events['completed']):
            decisions = self.get_decisions_after_successfull_workflow_execution()

        if not self.is_terminate_workflow() and events['completed']:
            decisions.extend(self.get_decisions_after_activity_completion(events['completed']))

        if not self.is_terminate_workflow() and events['decision_failed']:
            decisions.extend(self.get_decisions_decision_failed(events['decision_failed']))

        return decisions

    def get_decisions_after_workflow_start(self):
        logger.debug('DecisionBuilder.get_decisions_after_workflow_start()')
        decisions = []
        tasks = self.execution_graph.get_first_tasks()
        for t in tasks:
            decision = self.get_decision_schedule_activity(t)
            decisions.append(decision)
        return decisions

    def get_decisions_faulty_tasks(self, task_events):
        """Analyze the faulty tasks and their retry strategies. If a task is to be resubmitted,
        add a decision to the output.

        Parameters
        ----------
        task_events: list
            List of ActivityTask Failed/TimedOut events

        Returns
        -------
        list
            List of ScheduleActivityTask decision if the tasks are being resubmitted. 
            If not, a TerminateWorkflow decision is returned and the self.terminate_worklfow flag
            is set.
        """
        decisions = []
        for e in task_events:
            if self.is_terminate_workflow():
                break
            id_ = self.history.get_id_task_event(e)
            t = self.execution_graph.tasks_by_id[id_]
            if t.retry_strategy:
                failures = self.history.get_number_activity_failures(t)
                if t.retry_strategy.is_task_resubmitted(failures):
                    decision = self.get_decision_schedule_activity(t, is_failed_task=True)
                    decisions.append(decision)
                else:
                    reason = 'task_retry_limit_reached'
                    details = self.decision_input.get_details_failed_tasks(task_events)
                    decisions = self.get_decisions_after_failed_workflow_execution(reason, details)
            else:
                reason = 'task_failed'
                details = self.decision_input.get_details_failed_tasks(task_events)
                decisions = self.get_decisions_after_failed_workflow_execution(reason, details)
        return decisions

    def get_decisions_after_activity_completion(self, events):
        """Return the decisions based on the completed activities since the last decision task.
        Parameters
        ----------
        events: list
            List of ActivityTaskCompleted or TimerFired events
        """
        logger.debug('DecisionBuilder.get_decisions_after_activity_completion...')

        task_ids = [self.history.get_id_task_event(e) for e in events]
        tasks = self.get_tasks_to_be_scheduled(task_ids)

        decisions = []
        for t in tasks:
            decisions.append(self.get_decision_schedule_activity(t))
        return decisions

    def get_decisions_decision_failed(self, events_decision_failed):
        decisions = []
        for event in events_decision_failed:
            last_event_id = self.history.get_event_attributes(event)['startedEventId']
            first_event_id = self.history.get_id_previous_started(event)
            builder = floto.decider.DecisionBuilder(copy.deepcopy(self.initial_activity_tasks), 
                    self.default_activity_task_list)
            builder.first_event_id = first_event_id
            builder.last_event_id = last_event_id
            builder._set_history(self.history)
            decisions.extend(builder._collect_decisions())
        return decisions

    def get_decisions_after_successfull_workflow_execution(self):
        result = self.decision_input.get_workflow_result()
        d = floto.decisions.CompleteWorkflowExecution(result=result)
        self.workflow_complete = True
        return [d]

    def get_decision_schedule_activity(self, task, is_failed_task=False):
        if isinstance(task, floto.specs.task.ActivityTask):
            input_ = self.decision_input.get_input_task(task, is_failed_task)
            return self.get_decision_schedule_activity_task(task, input_)
        elif isinstance(task, floto.specs.task.ChildWorkflow):
            input_ = self.decision_input.get_input_task(task, is_failed_task)
            return self.get_decision_start_child_workflow_execution(task, input_)
        elif isinstance(task, floto.specs.task.Timer):
            return self.get_decision_start_timer(task)
        else:
            m = 'Do not know how to get decision for task of type: {}'.format(type(task))
            raise ValueError(m)

    def get_decisions_after_failed_workflow_execution(self, reason, details):
        d = floto.decisions.FailWorkflowExecution(details=details, reason=reason)
        self.workflow_fail = True
        return [d]

    def get_decision_schedule_activity_task(self, activity_task=None, input=None):
        activity_type = floto.api.ActivityType(name=activity_task.name,
                                               version=activity_task.version)
        task_list = activity_task.task_list or self.default_activity_task_list
        decision = floto.decisions.ScheduleActivityTask(activity_type=activity_type,
                activity_id=activity_task.id_,
                task_list=task_list, input=input)
        return decision

    def get_decision_start_timer(self, timer_task):
        return floto.decisions.StartTimer(timer_id=timer_task.id_,
                                          start_to_fire_timeout=timer_task.delay_in_seconds)

    def get_decision_start_child_workflow_execution(self, child_workflow_task=None, input_=None):
        logger.debug('DecisionBuilder.get_decision_start_child_workflow_execution...')
        workflow_type = floto.api.WorkflowType(name=child_workflow_task.workflow_type_name,
                version=child_workflow_task.workflow_type_version)
        args = {'workflow_id':child_workflow_task.id_,
                'workflow_type':workflow_type}
        if child_workflow_task.task_list:
            args['task_list'] = child_workflow_task.task_list
        if input_:
            args['input'] = input_
        return floto.decisions.StartChildWorkflowExecution(**args)

    def all_workflow_tasks_finished(self, completed_tasks):
        """Return True if all tasks of this workflow have finished, False otherwise."""
        logger.debug('DecisionBuilder.all_workflow_tasks_finished({})'.format((completed_tasks)))

        if self.completed_contain_generator(completed_tasks):
            return False
        if self.completed_have_depending_tasks(completed_tasks):
            return False
        if not self.outgoing_vertices_completed():
            return False
        return True

    def completed_contain_generator(self, completed_tasks):
        for task in completed_tasks:
            id_ = self.history.get_id_task_event(task)
            if isinstance(self.execution_graph.tasks_by_id[id_], floto.specs.task.Generator):
                return True

    def completed_have_depending_tasks(self, completed_tasks):
        """Return True if any of the tasks in "completed_tasks" has a task which depends on it.
        False otherwise."""
        logger.debug('DecisionBuilder.completed_have_depending_tasks({})'.format((completed_tasks)))
        for t in completed_tasks:
            id_ = self.history.get_id_task_event(t)
            depending_tasks = self.execution_graph.get_depending_tasks(id_)
            if depending_tasks:
                return True
        return False

    def outgoing_vertices_completed(self):
        """Check if all activity tasks which are outgoing vertices of the execution graph are
        completed."""
        outgoing_vertices = self.execution_graph.outgoing_vertices()
        for t in outgoing_vertices:
            if not self.history.is_task_completed(t):
                return False
        return True

    def get_tasks_to_be_scheduled(self, completed_tasks):
        """Based on a list of completed tasks, retrieve the tasks to be executed next.
        Parameter
        ---------
        list: floto.specs.Task
            List of completed tasks
        Returns
        -------
        list: floto.specs.Task
            The tasks to be scheduled in the next decision
        """
        logger.debug('DecisionBuilder.get_tasks_to_be_scheduled({})'.format(completed_tasks))
        tasks = []
        for completed_task in completed_tasks:
            tasks += self.get_tasks_to_be_scheduled_single_id(completed_task)
        tasks = self.uniqify_activity_tasks(tasks)
        return tasks

    def get_tasks_to_be_scheduled_single_id(self, activity_id):
        depending = self.execution_graph.get_depending_tasks(activity_id)
        tasks = []
        for d in depending:
            dependencies = self.execution_graph.get_dependencies(d.id_)
            if all([self.history.is_task_completed(t) for t in dependencies]):
                tasks.append(d)
        return tasks

    def uniqify_activity_tasks(self, activity_tasks):
        return list({t.id_: t for t in activity_tasks}.values())

    def _update_execution_graph_with_completed_events(self, completed_events):
        """Updates the execution graph if the completed activities contain generators."""
        for e in completed_events: 
            activity_id = self.history.get_id_task_event(e)
            g = self._get_generator(e)
            if g: 
                self._update_execution_graph(g)
                self.decision_input._execution_graph = self.execution_graph

    def _update_execution_graph(self, generator):
        """Updates the execution graph."""
        result_generator = self.history.get_result_completed_activity(generator)
        if self._decompress_generator_result:
            new_tasks_json = self._decompress_result(result_generator)
        else:
            new_tasks_json = json.dumps(result_generator)

        new_tasks = json.loads(new_tasks_json, object_hook=floto.specs.JSONEncoder.object_hook)
        self.execution_graph.update(generator, new_tasks)

    def _decompress_result(self, compressed_result):
        result_bytes = bytes([int(c, 16) for c in compressed_result.split('x')])
        result = gzip.decompress(result_bytes).decode()
        return result
            
    def _get_generator(self, completed_event):
        """Takes a completed event as defined by floto.History.get_events_for_decision and returns
        the corresponding floto.specs.task.Generator if generator is found with id_.
        Returns
        -------
        generator: <floto.specs.task.Generator>
        """
        g = None
        if completed_event['eventType'] == 'ActivityTaskCompleted':
            activity_id = self.history.get_id_task_event(completed_event)
            activity = self.execution_graph.tasks_by_id[activity_id]
            if isinstance(activity, floto.specs.task.Generator):
                g = activity
        return g

    def _build_execution_logic(self):
        copy_initial_tasks = copy.deepcopy(self.initial_activity_tasks)
        self._execution_graph = floto.decider.ExecutionGraph(copy_initial_tasks)
        self.decision_input._execution_graph = self.execution_graph

        if self.execution_graph.has_generator_task():
            events = self.history.get_events_for_decision(1, self.last_event_id)
            completed = events['completed']
            self._update_execution_graph_with_completed_events(completed)


