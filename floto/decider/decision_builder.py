import json

import floto
import floto.decisions
import floto.specs


class DecisionBuilder:
    def __init__(self, execution_graph, activity_task_list):
        self.workflow_fail = False
        self.workflow_complete = False
        self.execution_graph = execution_graph
        self.history = None
        self.activity_task_list = activity_task_list
        self.workflow_input = None
        self.current_workflow_execution_description = None

    def get_decisions(self, history):
        self.history = history
        self.workflow_fail = False
        self.workflow_complete = False

        first_event_id = self.history.previous_decision_id
        last_event_id = self.history.decision_task_started_event_id
        decisions = self._collect_decisions(first_event_id, last_event_id)
        return decisions

    def is_terminate_workflow(self):
        return self.workflow_fail or self.workflow_complete

    def _collect_decisions(self, first_event_id, last_event_id):
        if first_event_id == 0:
            return self.get_decisions_after_workflow_start()

        decisions = []
        events = self.history.get_events_for_decision(first_event_id, last_event_id)

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
        decisions = []
        tasks = self.execution_graph.get_first_tasks()
        for t in tasks:
            if isinstance(t, floto.specs.ActivityTask):
                input_ = self.get_input_activity_task_after_workflow_start(t)
                decision = self.get_decision_schedule_activity_task(activity_task=t, input=input_)
            else:
                decision = self.get_decision_start_timer(t)
            decisions.append(decision)
        return decisions

    def get_decisions_faulty_tasks(self, task_events):
        """Analyze the faulty tasks and their retry strategies. If a task is to be resubmitted,
        add a decision to the output

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
            activity_id = self.history.get_id_activity_task_event(e)
            t = self.execution_graph.tasks_by_id[activity_id]
            if t.retry_strategy:
                failures = self.history.get_number_activity_task_failures(activity_id)
                if t.retry_strategy.is_task_resubmitted(failures):
                    scheduled_event = self.history.get_event_task_scheduled(activity_id)
                    attributes = scheduled_event['activityTaskScheduledEventAttributes']
                    input = json.loads(attributes['input']) if 'input' in attributes else None
                    decision = self.get_decision_schedule_activity_task(t, input)
                    decisions.append(decision)
                else:
                    reason = 'task_retry_limit_reached'
                    details = self.get_details_failed_tasks(task_events)
                    decisions = self.get_decisions_after_failed_workflow_execution(reason, details)
            else:
                reason = 'task_failed'
                details = self.get_details_failed_tasks(task_events)
                decisions = self.get_decisions_after_failed_workflow_execution(reason, details)
        return decisions

    def get_decisions_after_activity_completion(self, events):
        """Return the decisions based on the completed activities since the last decision task.
        Parameters
        ----------
        events: list
            List of ActivityTaskCompleted or TimerFired events
        """
        task_ids = [self.history.get_id_task_event(e) for e in events]
        tasks = self.get_tasks_to_be_scheduled(task_ids)

        decisions = []
        for t in tasks:
            decisions.append(self.get_decision_task(t))
        return decisions

    def get_decisions_decision_failed(self, events_decision_failed):
        decisions = []
        for event in events_decision_failed:
            last_event_id = self.history.get_event_attributes(event)['startedEventId']
            first_event_id = self.history.get_id_previous_started(event)
            decisions.extend(self._collect_decisions(first_event_id, last_event_id))
        return decisions

    def get_decisions_after_successfull_workflow_execution(self):
        result = self.get_workflow_result()
        d = floto.decisions.CompleteWorkflowExecution(result=result)
        self.workflow_complete = True
        return [d]

    def get_decisions_after_failed_workflow_execution(self, reason, details):
        d = floto.decisions.FailWorkflowExecution(details=details, reason=reason)
        self.workflow_fail = True
        return [d]

    def get_decision_task(self, task):
        """Return a single decision for an ActivityTask or a Timer
        Parameters
        ----------
        task: floto.specs.ActivityTask or floto.specs.Timer

        Returns
        -------
        floto.decision.ScheduleActivityTask of floto.decision.StartTimer
        """
        if isinstance(task, floto.specs.ActivityTask):
            input_ = self.get_input_activity_task(task)
            return self.get_decision_schedule_activity_task(task, input_)
        elif isinstance(task, floto.specs.Timer):
            return self.get_decision_start_timer(task)

    def get_decision_schedule_activity_task(self, activity_task=None, input=None):
        activity_type = floto.api.ActivityType(name=activity_task.name,
                                               version=activity_task.version)
        decision = floto.decisions.ScheduleActivityTask(activity_type=activity_type,
                                                        activity_id=activity_task.id_,
                                                        task_list=self.activity_task_list, input=input)
        return decision

    def get_decision_start_timer(self, timer_task):
        return floto.decisions.StartTimer(timer_id=timer_task.id_,
                                          start_to_fire_timeout=timer_task.delay_in_seconds)

    def get_input_activity_task_after_workflow_start(self, task):
        if not self.workflow_input:
            self.workflow_input = self.history.get_workflow_input()
        input_ = {'workflow': self.workflow_input} if self.workflow_input else {}
        if task.input: input_['activity_task'] = task.input
        return input_

    def get_input_activity_task(self, task):
        if task.input:
            input_ = {'activity_task': task.input}
        else:
            input_ = {}
        dependencies = self.execution_graph.get_dependencies(task.id_)
        for d in dependencies:
            result = self.history.get_result_completed_activity(d)
            if result:
                input_[d.id_] = result
        return input_ if input_ else None

    def get_details_failed_tasks(self, failed_tasks_events):
        details = {}
        for e in failed_tasks_events:
            attributes = self.history.get_event_attributes(e)
            if 'details' in attributes:
                activity_id = self.history.get_id_activity_task_event(e)
                details[activity_id] = attributes['details']
        return details

    def get_workflow_result(self):
        outgoing_vertices = self.execution_graph.outgoing_vertices()
        result = {}
        for task in outgoing_vertices:
            r = self.history.get_result_completed_activity(task)
            if r:
                result[task.id_] = r
        return result if result else None

    def all_workflow_tasks_finished(self, completed_tasks):
        """Return True if all tasks of this workflow have finished, False otherwise."""
        if self.completed_have_depending_tasks(completed_tasks):
            return False
        if self.open_task_counts():
            return False
        if not self.outgoing_vertices_completed():
            return False
        return True

    def completed_have_depending_tasks(self, completed_tasks):
        """Return True if any of the tasks in "completed_tasks" has a task which depends on it.
        False otherwise."""
        for t in completed_tasks:
            id_ = self.history.get_id_task_event(t)
            depending_tasks = self.execution_graph.get_depending_tasks(id_)
            if depending_tasks:
                return True
        return False

    def open_task_counts(self):
        """Return True if there are "openActivityTasks" or "openTimers" in the "workflow execution
        description. False otherwise."""
        if not self.current_workflow_execution_description:
            return False

        description = self.current_workflow_execution_description
        open_activity_tasks = description['openCounts']['openActivityTasks']
        open_timers = description['openCounts']['openTimers']
        if open_activity_tasks or open_timers:
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
