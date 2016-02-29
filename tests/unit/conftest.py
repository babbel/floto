import pytest
import datetime as dt

@pytest.fixture
def init_response():
    dt1 =  dt.datetime(2016, 1, 12, hour=1, tzinfo=dt.timezone.utc)
    dt2 =  dt.datetime(2016, 1, 12, hour=2, tzinfo=dt.timezone.utc)
    dt3 =  dt.datetime(2016, 1, 12, hour=3, tzinfo=dt.timezone.utc)

    return { 'events': [ { 'decisionTaskStartedEventAttributes': { 'scheduledEventId': 2},
                 'eventId': 3,
                 'eventTimestamp': dt3,
                 'eventType': 'DecisionTaskStarted'},
               { 'decisionTaskScheduledEventAttributes': { 'startToCloseTimeout': '21600',
                                                           'taskList': { 'name': 'tl'},
                                                           'taskPriority': '0'},
                 'eventId': 2,
                 'eventTimestamp': dt2,
                 'eventType': 'DecisionTaskScheduled'},
               { 'eventId': 1,
                 'eventTimestamp': dt1,
                 'eventType': 'WorkflowExecutionStarted',
                 'workflowExecutionStartedEventAttributes':{'input':'workflow_input'}}],
             'previousStartedEventId': 0,
             'startedEventId': 3,
             'taskToken': 'val_task_token',
             'workflowExecution': { 'runId': 'val_run_id',
                                    'workflowId': 'val_workflow_id'},
             'workflowType': {'name': 'my_workflow_type', 'version': 'v1'}}

@pytest.fixture
def empty_response():
    return { 'previousStartedEventId': 0,
             'startedEventId': 3,
             'taskToken': 'val_task_token',
             'workflowExecution': { 'runId': 'val_run_id',
                                    'workflowId': 'val_workflow_id'},
             'workflowType': {'name': 'my_workflow_type', 'version': 'v1'}}

@pytest.fixture
def page1_response():
    dt2 =  dt.datetime(2016, 1, 12, hour=2, tzinfo=dt.timezone.utc)
    dt3 =  dt.datetime(2016, 1, 12, hour=3, tzinfo=dt.timezone.utc)

    return { 'events': [ { 'decisionTaskStartedEventAttributes': { 'scheduledEventId': 2},
                           'eventId': 3,
                           'eventTimestamp': dt3,
                           'eventType': 'DecisionTaskStarted'},
                         { 'eventId': 2,
                           'eventTimestamp': dt2,
                           'eventType': 'DecisionTaskCompleted'}],
             'nextPageToken': 'page2',
             'previousStartedEventId': 2,
             'startedEventId': 3,
             'taskToken': 'val_task_token',
             'workflowExecution': { 'runId': 'val_run_id',
                                    'workflowId': 'val_workflow_id'},
             'workflowType': {'name': 'my_workflow_type', 'version': 'v1'}}

@pytest.fixture
def page2_response():
    dt1 =  dt.datetime(2016, 1, 12, hour=1, tzinfo=dt.timezone.utc)

    return { 'events': [ { 'eventId': 1,
                           'eventTimestamp': dt1,
                           'eventType': 'WorkflowExecutionStarted',
                           'workflowExecutionStartedEventAttributes':{'input':'workflow_input'}}],
             'nextPageToken': 'page3',
             'previousStartedEventId': 0,
             'startedEventId': 3}

@pytest.fixture
def page1_decision_response():
    dt2 =  dt.datetime(2016, 1, 12, hour=2, tzinfo=dt.timezone.utc)
    return { 'events': [ { 'decisionTaskStartedEventAttributes': { 'scheduledEventId': 2},
                           'eventId': 3,
                           'eventTimestamp': dt2,
                           'eventType': 'DecisionTaskStarted'}],
             'nextPageToken': 'page2',
             'previousStartedEventId': 1,
             'startedEventId': 3,
             'taskToken': 'val_task_token',
             'workflowExecution': { 'runId': 'val_run_id',
                                    'workflowId': 'val_workflow_id'},
             'workflowType': {'name': 'my_workflow_type', 'version': 'v1'}}

@pytest.fixture
def page2_decision_response():
    dt1 =  dt.datetime(2016, 1, 12, hour=1, tzinfo=dt.timezone.utc)
    return { 'events': [ { 'decisionTaskStartedEventAttributes': { 'scheduledEventId': 2},
                           'eventId': 1,
                           'eventTimestamp': dt1,
                           'eventType': 'DecisionTaskStarted'}],
             'previousStartedEventId': 1,
             'startedEventId': 3,
             'taskToken': 'val_task_token',
             'workflowExecution': { 'runId': 'val_run_id',
                                    'workflowId': 'val_workflow_id'},
             'workflowType': {'name': 'my_workflow_type', 'version': 'v1'}}
