import pytest
from unittest.mock import PropertyMock, Mock

import json
import floto
import floto.specs.task
import datetime

class SwfMock:
    def __init__(self):
        self.pages = {}

    def poll_for_decision_task(self, **args):
        next_page_token =  args['nextPageToken']
        return self.pages[next_page_token]

@pytest.fixture
def dt1():
    return datetime.datetime(2016, 1, 12, hour=1, tzinfo=datetime.timezone.utc)

@pytest.fixture
def dt2():
    return datetime.datetime(2016, 1, 12, hour=2, tzinfo=datetime.timezone.utc)

@pytest.fixture
def dt3():
    return datetime.datetime(2016, 1, 12, hour=3, tzinfo=datetime.timezone.utc)

@pytest.fixture
def dt4():
    return datetime.datetime(2016, 1, 12, hour=4, tzinfo=datetime.timezone.utc)

@pytest.fixture
def dt5():
    return datetime.datetime(2016, 1, 12, hour=5, tzinfo=datetime.timezone.utc)

@pytest.fixture
def dt6():
    return datetime.datetime(2016, 1, 12, hour=6, tzinfo=datetime.timezone.utc)


@pytest.fixture
def history(init_response):
    return floto.History(domain='d', task_list='tl', response=init_response)

class TestHistory():
    def test_init(self, init_response):
        h = floto.History(domain='d', task_list='tl', response=init_response)
        assert h.domain == 'd'
        assert h.task_list == 'tl'
        assert h.next_page_token == None 
        assert h.highest_event_id == 3
        assert h.lowest_event_id == 1
        assert h.decision_task_started_event_id == 3
        assert h.previous_decision_id == 0

    def test_events_by_id(self, init_response):
        h = floto.History(domain='d', task_list='tl', response=init_response)
        assert h.events_by_id[1]['eventType'] == 'WorkflowExecutionStarted'

    def test_events_by_id_multi_page(self, mocker, page1_response, page2_response):
        swf_mock = SwfMock()
        swf_mock.pages['page2'] = page2_response
        mocker.patch('floto.api.Swf.client', new_callable=PropertyMock, return_value=swf_mock)
        h = floto.History(domain='d', task_list='tl', response=page1_response)
        assert h.lowest_event_id == 2
        assert h.get_event(1)
        assert h.lowest_event_id == 1

    def test_events_by_id_forbid_next_page(self, mocker, page1_response, page2_response):
        swf_mock = SwfMock()
        swf_mock.pages['page2'] = page2_response
        mocker.patch('floto.api.Swf.client', new_callable=PropertyMock, return_value=swf_mock)
        h = floto.History(domain='d', task_list='tl', response=page1_response)
        assert h.lowest_event_id == 2
        assert h.get_event(1, allow_read_next_event_page=False) == None
        assert h.lowest_event_id == 2

    def test_get_events_by_type(self, history):
        assert history.get_events_by_type('WorkflowExecutionStarted')[0]['eventId'] == 1
        assert history.get_events_by_type('foo') == []

    def test_get_events_for_decision(self, history):
        raw_events = [{'eventId':12, 'eventType':'ChildWorkflowExecutionCompleted'},
                      {'eventId':11, 'eventType':'ChildWorkflowExecutionTerminated'},
                      {'eventId':10, 'eventType':'ChildWorkflowExecutionCanceled'},
                      {'eventId':9, 'eventType':'ChildWorkflowExecutionTimedOut'},
                      {'eventId':8, 'eventType':'ChildWorkflowExecutionFailed'},
                      {'eventId':7, 'eventType':'ActivityTaskFailed'},
                      {'eventId':6, 'eventType':'DecisionTaskTimedOut'},
                      {'eventId':5, 'eventType':'TimerFired'},
                      {'eventId':4, 'eventType':'ActivityTaskCompleted'},
                      {'eventId':3, 'eventType':'ActivityTaskTimedOut'},
                      {'eventId':2, 'eventType':'ActivityTaskFailed'},
                      {'eventId':1, 'eventType':'ActivityTaskFailed'},
                      {'eventId':0, 'eventType':'WorkflowExecutionStarted'}]

        history.events_by_id = {e['eventId']:e for e in raw_events}
        history.highest_event_id = 12
        events = history.get_events_for_decision(1,12)
        assert len(events['faulty']) == 8
        assert set([e['eventId'] for e in events['faulty']]) == set([1,2,3,7,8,9,10,11])
        assert set([e['eventId'] for e in events['completed']]) == set([4,5,12])
        assert set([e['eventId'] for e in events['decision_failed']]) == set([6])

    def test_event_by_activity_id(self, dt1, dt2, empty_response):
        events = [{'eventId':2, 
                   'eventType':'ActivityTaskCompleted', 
                   'eventTimestamp':dt2,
                   'activityTaskCompletedEventAttributes':{'scheduledEventId':1}},
                  {'eventId':1,
                   'eventType':'ActivityTaskScheduled',
                   'eventTimestamp':dt1,
                   'activityTaskScheduledEventAttributes':{'activityId':'a_id'}}]
        empty_response['events'] = events
        h = floto.History(domain='d', task_list='tl', response=empty_response)
        assert h.events_by_activity_id['a_id']['ActivityTaskCompleted'][0]['eventId'] == 2

    def test_event_by_activity_id_wo_scheduled_event(self, empty_response, dt1, dt2):
        events = [{'eventId':3, 
                   'eventType':'ActivityTaskCompleted', 
                   'eventTimestamp':dt2,
                   'activityTaskCompletedEventAttributes':{'scheduledEventId':1}},
                  {'eventId':2,
                   'eventType':'DecisionTaskCompleted',
                   'eventTimestamp':dt1}]
        empty_response['events'] = events
        empty_response['previousStartedEventId'] = 2
        h = floto.History(domain='d', task_list='tl', response=empty_response)
        assert h.events_by_activity_id['none']['ActivityTaskCompleted'][0]['eventId'] == 3

    def test_event_by_activity_id_multi_page(self, dt1, dt2, dt3, dt4, dt5, page1_response, 
            page2_response, mocker):
        page1_events = [{'eventId':7, 
                         'eventType':'ActivityTaskCompleted', 
                         'eventTimestamp':dt5,
                         'activityTaskCompletedEventAttributes':{'scheduledEventId':6}},
                        {'eventId':6,
                         'eventType':'ActivityTaskScheduled',
                         'eventTimestamp':dt4,
                         'activityTaskScheduledEventAttributes':{'activityId':'A'}},
                        {'eventId':5,
                         'eventType':'ActivityTaskFailed',
                         'eventTimestamp':dt3,
                         'activityTaskFailedEventAttributes':{'scheduledEventId':3}},
                        {'eventId':4,
                         'eventType':'DecisionTaskCompleted',
                         'eventTimestamp':dt2}]
        page2_events = [{'eventId':3,
                         'eventType':'ActivityTaskScheduled',
                         'eventTimestamp':dt2,
                         'activityTaskScheduledEventAttributes':{'activityId':'B'}},
                        {'eventId':2,
                         'eventType':'ActivityTaskTimedOut',
                         'eventTimestamp':dt1,
                         'activityTaskTimedOutEventAttributes':{'scheduledEventId':1}},
                        {'eventId':1,
                         'eventType':'ActivityTaskScheduled',
                         'eventTimestamp':dt1,
                         'activityTaskScheduledEventAttributes':{'activityId':'C'}}]
                         
        page1_response['events'] = page1_events
        page2_response['events'] = page2_events
        page1_response['previousStartedEventId'] = 4

        swf_mock = SwfMock()
        swf_mock.pages['page2'] = page2_response
        mocker.patch('floto.api.Swf.client', new_callable=PropertyMock, return_value=swf_mock)
        
        h = floto.History(domain='d', task_list='tl', response=page1_response)
        assert h.events_by_activity_id['A']['ActivityTaskCompleted'][0]['eventId'] == 7
        assert h.events_by_activity_id['none']['ActivityTaskFailed'][0]['eventId'] == 5
        h._read_next_event_page()
        assert h.events_by_activity_id['A']['ActivityTaskCompleted'][0]['eventId'] == 7
        assert h.events_by_activity_id['A']['ActivityTaskScheduled'][0]['eventId'] == 6
        assert h.events_by_activity_id['B']['ActivityTaskFailed'][0]['eventId'] == 5
        assert len(h.events_by_activity_id['A']) == 2
        assert h.events_by_activity_id['C']['ActivityTaskTimedOut'][0]['eventId'] == 2

    def test_is_first_decision_task(self, history):
        history.previous_decision_id = 0
        assert history.is_first_decision_task()

    def test_is_not_first_decision_task(self, history):
        history.previous_decision_id = 1
        assert not history.is_first_decision_task()

    def test_get_datetime_previous_decision_wo(self, empty_response, dt1):
        empty_response['events'] = [{'eventId':1, 
                                     'eventType':'WorkflowExecutionStarted', 
                                     'eventTimestamp':dt1}]
        h = floto.History(domain='d', task_list='tl', response=empty_response)
        assert h.get_datetime_previous_decision() == dt1

    def test_get_datetime_previous_decision_contained_in_page(self, empty_response, dt1, dt2):
        events = [ { 'eventId': 2,
                     'eventTimestamp': dt2,
                     'eventType': 'DecisionTaskStarted'},
                   { 'eventId': 1,
                     'eventTimestamp': dt1, 
                     'eventType': 'WorkflowExecutionStarted'}]
        empty_response['events'] = events
        empty_response['previousStartedEventId'] = 2
        h = floto.History(domain='d', task_list='tl', response=empty_response)
        assert h.get_datetime_previous_decision() == dt2

    def test_get_datetime_previous_decision_contained_in_page2(self, mocker, page1_decision_response,
            page2_decision_response):
        swf_mock = SwfMock()
        swf_mock.pages['page2'] = page2_decision_response
        mocker.patch('floto.api.Swf.client', new_callable=PropertyMock, return_value=swf_mock)
        
        h = floto.History(domain='d', task_list='tl', response=page1_decision_response)
        dt1 =  datetime.datetime(2016, 1, 12, hour=1, tzinfo=datetime.timezone.utc)
        h.get_datetime_previous_decision() == dt1

    def test_get_datetime_last_event_of_activitiy(self, mocker, history, dt1):
        event = {'eventTimestamp':dt1}
        mocker.patch('floto.History.get_events_by_task_id_and_type', return_value=[event])
        assert history.get_datetime_last_event_of_activity('a_id', 'TaskCompleted') == dt1
        history.get_events_by_task_id_and_type.assert_called_once_with('a_id', 'TaskCompleted')

    def test_read_events_up_to_last_decision(self, mocker, page1_decision_response,
            page2_decision_response):
        swf_mock = SwfMock()
        swf_mock.pages['page2'] = page2_decision_response
        mocker.patch('floto.api.Swf.client', new_callable=PropertyMock, return_value=swf_mock)
        
        h = floto.History(domain='d', task_list='tl', response=page1_decision_response)
        assert h.events_by_id[1]
        
    def test_read_next_event_page_raises(self, init_response):
        h = floto.History(domain='d', task_list='tl', response=init_response)
        h.next_page_token = None
        with pytest.raises(ValueError):
            h._read_next_event_page()

    def test_read_event_page(self, history):
        history.events_by_id = {}
        history.events_by_type = {}
        events = [{'eventId':1, 'eventType':'type1'},
                  {'eventId':2, 'eventType':'type1'},
                  {'eventId':3, 'eventType':'type2'}]
        history._read_event_page(events)
        assert history.events_by_id[3]['eventType'] == 'type2'
        assert history.events_by_type['type1'][0]['eventId'] == 1
        assert history.events_by_type['type1'][1]['eventId'] == 2
        assert history.events_by_type['type2'][0]['eventId'] == 3

    def test_read_next_event_page(self, mocker, page1_response, page2_response):
        swf_mock = SwfMock()
        swf_mock.pages['page2'] = page2_response
        mocker.patch('floto.api.Swf.client', new_callable=PropertyMock, return_value=swf_mock)
        
        h = floto.History(domain='d', task_list='tl', response=page1_response)
        h._read_next_event_page()
        assert h.events_by_id[1]['eventType'] == 'WorkflowExecutionStarted'
        assert h.next_page_token == 'page3'
        
    def test_get_event_up_to_last_decision_empty(self, history):
        dt2 =  datetime.datetime(2016, 1, 12, hour=2, tzinfo=datetime.timezone.utc)
        history.dt_previous_decision_task = dt2
        assert len(history.get_events_up_to_last_decision('DecisionTaskStarted')) == 1
        assert len(history.get_events_up_to_last_decision('DecisionTaskScheduled')) == 1
        assert len(history.get_events_up_to_last_decision('WorkflowExecutionStarted')) == 0

    def test__has_next_event_page_false(self, init_response):
        h = floto.History(domain='d', task_list='tl', response=init_response)
        assert h._has_next_event_page() == False

    def test__has_next_event_page_true(self, dt1, empty_response):
        events = [{'eventId':1, 'eventType':'DecisionTaskCompleted', 'eventTimestamp':dt1}]
        empty_response['events'] = events
        empty_response['previousStartedEvent'] = 1
        empty_response['nextPageToken'] = 'page2'
        h = floto.History(domain='d', task_list='tl', response=empty_response)
        assert h._has_next_event_page() == True

    def test_is_activity_task_completed(self, init_response, dt1, dt2, dt3):
        events = [{'eventId':3, 
                   'eventType':'ActivityTaskCompleted', 
                   'eventTimestamp':dt3,
                   'activityTaskCompletedEventAttributes':{'scheduledEventId':2}},
                  {'eventId':2,
                   'eventType':'ActivityTaskScheduled',
                   'eventTimestamp':dt2,
                   'activityTaskScheduledEventAttributes':{'activityId':'a_id'}},
                  {'eventId':1,
                   'eventType':'WorkflowExecutionStarted',
                   'eventTimestamp':dt1}]
        init_response['events'] = events
        h = floto.History(domain='d', task_list='tl', response=init_response)
        assert h.is_activity_task_completed('a_id')

    def test_task_completed_with_activity_task(self, mocker, history):
        mocker.patch('floto.History.is_activity_task_completed', return_value=True)
        activity_task = floto.specs.task.ActivityTask(domain='d', name='a', version='1')
        assert history.is_task_completed(activity_task)

    def test_task_complete_with_timer(self, mocker, history):
        mocker.patch('floto.History.is_timer_task_completed', return_value=True)
        timer = floto.specs.task.Timer(id_='t1')
        assert history.is_task_completed(timer)

    def test_task_complete_with_child_workflow(self, mocker, history):
        mocker.patch('floto.History.is_child_workflow_completed', return_value=True)
        cw = floto.specs.task.ChildWorkflow(domain='d', workflow_type_name='cw', workflow_type_version='1')
        assert history.is_task_completed(cw)

    def test_is_task_completed_raises(self, history):
        with pytest.raises(ValueError):
            history.is_task_completed('foo')

    def test_is_timer_task_completed_without_start(self, history):
        assert not history.is_timer_task_completed('t_id')

    def test_is_timer_task_completed_with_started_event(self, init_response, dt1):
        events = [{'eventId':1, 
                   'eventType':'TimerStarted', 
                   'eventTimestamp':dt1,
                   'timerStartedEventAttributes':{'timerId':'t_id'}}]
        init_response['events'] = events
        h = floto.History(domain='d', task_list='tl', response=init_response)
        assert not h.is_timer_task_completed('t_id')

    def test_is_timer_task_completed_with_fired_event(self, init_response, dt1, dt2):
        events = [{'eventId':2, 
                   'eventType':'TimerFired', 
                   'eventTimestamp':dt2,
                   'timerFiredEventAttributes':{'timerId':'t_id'}},
                   {'eventId':1, 
                   'eventType':'TimerStarted', 
                   'eventTimestamp':dt1,
                   'timerStartedEventAttributes':{'timerId':'t_id'}}]
        init_response['events'] = events
        h = floto.History(domain='d', task_list='tl', response=init_response)
        assert h.is_timer_task_completed('t_id')
        
    def test_is_timer_task_completed_multiple_pages(self, page1_response, page2_response, dt1, dt2,
            dt3, mocker):
        page1_response['events'] = [{'eventId':3,
                                     'eventType':'TimerFired',
                                     'eventTimestamp':dt3,
                                     'timerFiredEventAttributes':{'timerId':'t_id'}},
                                    {'eventId':2, 
                                     'eventType':'DecisionTaskCompleted', 
                                     'eventTimestamp':dt2}]
        page2_response['events'] = [{'eventId':1, 
                                     'eventType':'TimerStarted', 
                                     'eventTimestamp':dt1,
                                     'timerStartedEventAttributes':{'timerId':'t_id'}}]

        page2_response.pop('nextPageToken', None)

        swf_mock = SwfMock()
        swf_mock.pages['page2'] = page2_response
        mocker.patch('floto.api.Swf.client', new_callable=PropertyMock, return_value=swf_mock)

        h = floto.History(domain='d', task_list='tl', response=page1_response)
        assert h._has_next_event_page()
        assert h.is_timer_task_completed('t_id')
        assert not h._has_next_event_page()

    def test_is_child_workflow_completed(self, mocker, dt1, dt2, history):
        completed_event = {'eventType':'ChildWorkflowExecutionCompleted',
                'eventTimestamp':dt2}
        initiated_event = {'eventType':'StartChildWorkflowExecutionInitiated',
                'eventTimestamp':dt1}

        events = {'ChildWorkflowExecutionCompleted':[completed_event],
                'StartChildWorkflowExecutionInitiated': [initiated_event]}

        mock_fct = lambda id_, type_: events[type_]
        history.get_events_by_task_id_and_type = mock_fct
        assert history.is_child_workflow_completed('wid')

    def test_get_event_by_task_id_and_type_no_event(self, init_response, dt1, dt2):
        events = [{'eventId':3, 
                   'eventType':'ActivityTaskCompleted', 
                   'eventTimestamp':dt2,
                   'activityTaskCompletedEventAttributes':{'activityId':'a_id',
                                                           'scheduledEventId':1}},
                  {'eventId':2,
                   'eventType':'DecisionTaskCompleted',
                   'eventTimestamp':dt1}]

        init_response['events'] = events
        init_response['previousStartedEventId'] = 1
        h = floto.History(domain='d', task_list='tl', response=init_response)
        assert not h.get_event_by_task_id_and_type('a_id', 'ActivityTaskScheduled')

    def test_get_event_by_task_id_and_type_single_event(self, init_response, dt1):
        events = [{'eventId':1, 
                   'eventType':'ActivityTaskScheduled', 
                   'eventTimestamp':dt1,
                   'activityTaskScheduledEventAttributes':{'activityId':'a_id'}}]
        init_response['events'] = events
        h = floto.History(domain='d', task_list='tl', response=init_response)
        assert h.get_event_by_task_id_and_type('a_id', 'ActivityTaskScheduled')['eventId'] == 1

    def test_get_event_by_task_id_several_events(self, init_response, dt1, dt2):
        events = [{'eventId':2,
                   'eventType':'ActivityTaskScheduled',
                   'eventTimestamp':dt2,
                   'activityTaskScheduledEventAttributes':{'activityId':'a_id_2'}},
                  {'eventId':1, 
                   'eventType':'ActivityTaskScheduled', 
                   'eventTimestamp':dt1,
                   'activityTaskScheduledEventAttributes':{'activityId':'a_id_1'}}]
        init_response['events'] = events
        h = floto.History(domain='d', task_list='tl', response=init_response)
        assert h.get_event_by_task_id_and_type('a_id_2', 'ActivityTaskScheduled')['eventId'] == 2

    def test_get_event_by_task_id_and_type_next_page(self, mocker, page1_response, page2_response):
        events = [{'eventId':1, 
                   'eventType':'ActivityTaskScheduled', 
                   'eventTimestamp':dt1,
                   'activityTaskScheduledEventAttributes':{'activityId':'a_id'}}]
        page2_response['events'] = events

        swf_mock = SwfMock()
        swf_mock.pages['page2'] = page2_response
        mocker.patch('floto.api.Swf.client', new_callable=PropertyMock, return_value=swf_mock)
        h = floto.History(domain='d', task_list='tl', response=page1_response)
        assert h.get_event_by_task_id_and_type('a_id', 'ActivityTaskScheduled')['eventId'] == 1

    @pytest.mark.parametrize('type_', [('ActivityTaskFailed'),
                                       ('ActivityTaskTimedOut'),
                                       ('ActivityTaskCompleted')])
    def test_get_id_task_event_with_activity_task(self, type_, history, mocker):
        mocker.patch('floto.History.get_id_activity_task_event', return_value='a_id')
        id_ = history.get_id_task_event({'eventType':type_})
        assert id_ == 'a_id'

    def test_get_id_task_event_with_timer_fired(self, mocker, history):
        mocker.patch('floto.History.get_id_timer_fired_event', return_value='t_id')
        id_ = history.get_id_task_event({'eventType':'TimerFired'})
        assert id_ == 't_id'

    def test_get_id_task_event_with_activity_task_scheduled(self, mocker, history):
        mocker.patch('floto.History.get_id_activity_task_scheduled', return_value='s_id')
        id_ = history.get_id_task_event({'eventType':'ActivityTaskScheduled'})
        assert id_ == 's_id'

    def test_get_task_event_raises(self, history):
        with pytest.raises(ValueError):
            history.get_id_task_event({'eventType':'Unknown'})

    @pytest.mark.parametrize('event_type',['ChildWorkflowExecutionFailed', 
        'ChildWorkflowExecutionTimedOut',
        'ChildWorkflowExecutionCanceled',
        'ChildWorkflowExecutionTerminated',
        'ChildWorkflowExecutionCompleted'])
    def test_get_id_task_event_child_workflow_event(self, history, mocker, event_type):
        mocker.patch('floto.History.get_id_child_workflow_event', return_value='cw_id')
        assert history.get_id_task_event({'eventType':event_type}) == 'cw_id'

    def test_get_id_task_event_child_workflow_initiated(self, history, mocker):
        mocker.patch('floto.History.get_id_start_child_workflow_execution_initiated', 
                return_value='cw_id')
        event_type = 'StartChildWorkflowExecutionInitiated'
        assert history.get_id_task_event({'eventType':event_type}) == 'cw_id'

    def test_get_id_start_child_workflow_execution_initiated(self, history):
        event = {'eventId':3,
                'eventType':'StartChildWorkflowExecutionInitiated',
                'startChildWorkflowExecutionInitiatedEventAttributes':{'workflowId':'wid'}}
        assert history.get_id_start_child_workflow_execution_initiated(event) == 'wid'

    def test_get_id_child_workflow_event(self, history):
        event = {'eventId':3,
                'eventType':'ChildWorkflowExecutionFailed',
                'childWorkflowExecutionFailedEventAttributes':{'workflowExecution':
                    {'workflowId':'wid'}}}
        assert history.get_id_child_workflow_event(event) == 'wid'

    def test_get_id_previous_started(self, empty_response, dt1, dt2):
        started_2 = {'eventId':2,
                     'eventType':'DecisionTaskStarted',
                     'eventTimestamp':dt2}
        started_1 = {'eventId':1,
                     'eventType':'DecisionTaskStarted',
                     'eventTimestamp':dt1}
        empty_response['events'] = [started_2, started_1]
        h = floto.History(domain='d', task_list='tl', response=empty_response)
        assert h.get_id_previous_started(started_2) == 1

    def test_get_id_previous_started_workflow_start(self, history, dt3):
        started_event = {'eventId': 3,
                         'eventTimestamp': dt3,
                         'eventType': 'DecisionTaskStarted'}
        assert history.get_id_previous_started(started_event) == 0

    def test_get_id_previous_started_multi_page(self, page1_response, page2_response, dt1, dt2,
            dt3, mocker):
        page1_response['events'] = [{'eventId':3,
                                     'eventType':'DecisionTaskStarted',
                                     'eventTimestamp':dt3},
                                    {'eventId':2, 
                                     'eventType':'DecisionTaskCompleted', 
                                     'eventTimestamp':dt2}]
        page2_response['events'] = [{'eventId':1, 
                                     'eventType':'DecisionTaskStarted', 
                                     'eventTimestamp':dt1}]

        swf_mock = SwfMock()
        swf_mock.pages['page2'] = page2_response
        mocker.patch('floto.api.Swf.client', new_callable=PropertyMock, return_value=swf_mock)

        h = floto.History(domain='d', task_list='tl', response=page1_response)
        assert h.get_id_previous_started(page1_response['events'][0]) == 1

    def test_get_id_timer_fired_event(self, history):
        timer_event = {'eventType':'TimerFired',
                       'timerFiredEventAttributes':{'timerId':'t_id'}}
        id_ = history.get_id_timer_fired_event(timer_event)
        assert id_ == 't_id'

    def test_get_id_activity_task_event_for_completed(self, empty_response, dt1, dt2):
        activity_task_completed_event = {'eventId':2,
                'eventType':'ActivityTaskCompleted',
                'eventTimestamp':dt2,
                'activityTaskCompletedEventAttributes':{'scheduledEventId':1}}
        events = [ activity_task_completed_event,
                  {'eventId':1,
                   'eventType':'ActivityTaskScheduled',
                   'eventTimestamp':dt1,
                   'activityTaskScheduledEventAttributes':{'activityId':'a_id'}}]
        empty_response['events'] = events 
        h = floto.History(domain='d', task_list='tl', response=empty_response)
        assert h.get_id_activity_task_event(activity_task_completed_event) == 'a_id'

    def test_get_id_activity_task_event_for_failed(self, empty_response, dt1, dt2):
        activity_task_failed_event = {'eventId':2,
                'eventType':'ActivityTaskFailed',
                'eventTimestamp':dt2,
                'activityTaskFailedEventAttributes':{'scheduledEventId':1}}

        events = [ activity_task_failed_event,
                  {'eventId':1,
                   'eventType':'ActivityTaskScheduled',
                   'eventTimestamp':dt1,
                   'activityTaskScheduledEventAttributes':{'activityId':'a_id'}}]
        empty_response['events'] = events 
        h = floto.History(domain='d', task_list='tl', response=empty_response)
        assert h.get_id_activity_task_event(activity_task_failed_event) == 'a_id'

    def test_get_id_activity_task_event_multi_page(self, page1_response, page2_response, 
            dt1, dt2, dt3, mocker):
        activity_task_failed_event = {'eventId':3,
                'eventType':'ActivityTaskFailed',
                'eventTimestamp':dt3,
                'activityTaskFailedEventAttributes':{'scheduledEventId':1}}

        decision_task_completed = {'eventId':2,
                'eventType':'DecisionTaskCompleted',
                'eventTimestamp':dt2}

        activity_task_scheduled_event = {'eventId':1,
                'eventType':'ActivityTaskScheduled',
                'eventTimestamp':dt1,
                'activityTaskScheduledEventAttributes':{'activityId':'a_id'}}

        page1_response['events'] = [activity_task_failed_event, decision_task_completed]
        page2_response['events'] = [activity_task_scheduled_event]

        swf_mock = SwfMock()
        swf_mock.pages['page2'] = page2_response
        mocker.patch('floto.api.Swf.client', new_callable=PropertyMock, return_value=swf_mock)

        h = floto.History(domain='d', task_list='tl', response=page1_response)
        assert h.get_id_activity_task_event(activity_task_failed_event) == 'a_id'

    def test_get_id_activity_task_event_multi_page_forbid_next_page(self, page1_response, 
            page2_response, dt1, dt2, dt3, mocker):
        activity_task_failed_event = {'eventId':3,
                'eventType':'ActivityTaskFailed',
                'eventTimestamp':dt3,
                'activityTaskFailedEventAttributes':{'scheduledEventId':1}}

        decision_task_completed = {'eventId':2,
                'eventType':'DecisionTaskCompleted',
                'eventTimestamp':dt2}

        activity_task_scheduled_event = {'eventId':1,
                'eventType':'ActivityTaskScheduled',
                'eventTimestamp':dt1,
                'activityTaskScheduledEventAttributes':{'activityId':'a_id'}}

        page1_response['events'] = [activity_task_failed_event, decision_task_completed]
        page2_response['events'] = [activity_task_scheduled_event]

        swf_mock = SwfMock()
        swf_mock.pages['page2'] = page2_response
        mocker.patch('floto.api.Swf.client', new_callable=PropertyMock, return_value=swf_mock)

        h = floto.History(domain='d', task_list='tl', response=page1_response)
        activity_id =  h.get_id_activity_task_event(activity_task_failed_event, 
                allow_read_next_event_page=False)
        assert activity_id == None

    def test_get_number_activity_failures_activity_task(self, history, mocker):
        mocker.patch('floto.History.get_number_activity_task_failures')
        history.get_number_activity_failures(floto.specs.task.ActivityTask(domain='d', name='a', version='1', activity_id='tid'))
        history.get_number_activity_task_failures.assert_called_once_with('tid')

    def test_get_number_activity_failures_child_workflow(self, history, mocker):
        mocker.patch('floto.History.get_number_child_workflow_failures')
        history.get_number_activity_failures(floto.specs.task.ChildWorkflow(domain='d',
                                                                            workflow_type_name='cw',
                                                                            workflow_type_version='1',
                                                                            workflow_id='wid'))
        history.get_number_child_workflow_failures.assert_called_once_with('wid')

    def test_get_number_activity_failures_unknown_type(self, history):
        assert history.get_number_activity_failures('task') == 0

    def test_get_number_activity_task_failures(self, empty_response, dt1, dt2, dt3):
        activity_task_timed_out_event = {'eventId':3,
                'eventType':'ActivityTaskTimedOut',
                'eventTimestamp':dt3,
                'activityTaskTimedOutEventAttributes':{'scheduledEventId':1}}
        activity_task_failed_event = {'eventId':2,
                'eventType':'ActivityTaskFailed',
                'eventTimestamp':dt2,
                'activityTaskFailedEventAttributes':{'scheduledEventId':1}}

        events = [activity_task_timed_out_event,
                  activity_task_failed_event,
                  {'eventId':1,
                   'eventType':'ActivityTaskScheduled',
                   'eventTimestamp':dt1,
                   'activityTaskScheduledEventAttributes':{'activityId':'a_id'}}]
        empty_response['events'] = events 
        h = floto.History(domain='d', task_list='tl', response=empty_response)
        assert h.get_number_activity_task_failures('a_id') == 2

    def test_get_number_activity_task_failures_after_completed(self, empty_response, dt1, dt2, dt3,
            dt4, dt5):
        activity_task_failed_event = {'eventId':5,
                'eventType':'ActivityTaskFailed',
                'eventTimestamp':dt5,
                'activityTaskFailedEventAttributes':{'scheduledEventId':4}}

        activity_task_scheduled_event_2 = {'eventId':4,
                   'eventType':'ActivityTaskScheduled',
                   'eventTimestamp':dt4,
                   'activityTaskScheduledEventAttributes':{'activityId':'a_id'}}

        activity_task_completed_event = {'eventId':3,
                'eventType':'ActivityTaskCompleted',
                'eventTimestamp':dt3,
                'activityTaskCompletedEventAttributes':{'scheduledEventId':1}}

        activity_task_timed_out_event = {'eventId':2,
                'eventType':'ActivityTaskTimedOut',
                'eventTimestamp':dt2,
                'activityTaskTimedOutEventAttributes':{'scheduledEventId':1}}

        activity_task_scheduled_event_1 = {'eventId':1,
                   'eventType':'ActivityTaskScheduled',
                   'eventTimestamp':dt1,
                   'activityTaskScheduledEventAttributes':{'activityId':'a_id'}}

        events = [activity_task_failed_event,
                  activity_task_scheduled_event_2,
                  activity_task_completed_event,
                  activity_task_timed_out_event,
                  activity_task_scheduled_event_1]

        empty_response['events'] = events 
        h = floto.History(domain='d', task_list='tl', response=empty_response)
        assert h.get_number_activity_task_failures('a_id') == 1

    def test_get_number_activity_task_failures_after_completed_two_pages(self, page1_response, 
            page2_response, dt1, dt2, dt3, dt4, dt5, dt6, mocker):
        activity_task_failed_event = {'eventId':7,
                'eventType':'ActivityTaskFailed',
                'eventTimestamp':dt6,
                'activityTaskFailedEventAttributes':{'scheduledEventId':4}}

        decision_task_completed = {'eventId':6,
                'eventType':'DecisionTaskCompleted',
                'eventTimestamp':dt5}

        activity_task_failed_event_2 = {'eventId':5,
                'eventType':'ActivityTaskFailed',
                'eventTimestamp':dt5,
                'activityTaskFailedEventAttributes':{'scheduledEventId':4}}

        activity_task_scheduled_event_2 = {'eventId':4,
                   'eventType':'ActivityTaskScheduled',
                   'eventTimestamp':dt4,
                   'activityTaskScheduledEventAttributes':{'activityId':'a_id'}}

        activity_task_completed_event = {'eventId':3,
                'eventType':'ActivityTaskCompleted',
                'eventTimestamp':dt3,
                'activityTaskCompletedEventAttributes':{'scheduledEventId':1}}

        activity_task_timed_out_event = {'eventId':2,
                'eventType':'ActivityTaskTimedOut',
                'eventTimestamp':dt2,
                'activityTaskTimedOutEventAttributes':{'scheduledEventId':1}}

        activity_task_scheduled_event_1 = {'eventId':1,
                   'eventType':'ActivityTaskScheduled',
                   'eventTimestamp':dt1,
                   'activityTaskScheduledEventAttributes':{'activityId':'a_id'}}

        page1_response['events'] = [activity_task_failed_event, 
                                    decision_task_completed]
        page2_response['events'] = [activity_task_failed_event_2,
                                    activity_task_scheduled_event_2,
                                    activity_task_completed_event,
                                    activity_task_timed_out_event,
                                    activity_task_scheduled_event_1]

        swf_mock = SwfMock()
        swf_mock.pages['page2'] = page2_response
        mocker.patch('floto.api.Swf.client', new_callable=PropertyMock, return_value=swf_mock)

        h = floto.History(domain='d', task_list='tl', response=page1_response)
        assert h.get_number_activity_task_failures('a_id') == 2

    def test_get_number_child_workflow_failures(self, empty_response, dt1, dt2, dt3):
        child_workflow_timed_out_event = {'eventId':3,
                'eventType':'ChildWorkflowExecutionTimedOut',
                'eventTimestamp':dt3,
                'childWorkflowExecutionTimedOutEventAttributes':
                    {'workflowExecution':{'workflowId':'wid'}}}

        child_workflow_failed_event = {'eventId':2,
                'eventType':'ChildWorkflowExecutionFailed',
                'eventTimestamp':dt2,
                'childWorkflowExecutionFailedEventAttributes':
                    {'workflowExecution':{'workflowId':'wid'}}}

        child_workflow_initiated_event = {'eventId':1,
                   'eventType':'StartChildWorkflowExecutionInitiated',
                   'eventTimestamp':dt1,
                   'startChildWorkflowExecutionInitiatedEventAttributes':{'workflowId':'wid'}}

        events = [child_workflow_timed_out_event,
                  child_workflow_failed_event,
                  child_workflow_initiated_event]
        empty_response['events'] = events 
        h = floto.History(domain='d', task_list='tl', response=empty_response)
        assert h.get_number_child_workflow_failures('wid') == 2

    def test_get_number_child_workflow_failures_after_completion(self, empty_response, dt1, dt2, 
            dt3, dt4):
        child_workflow_timed_out_event = {'eventId':4,
                'eventType':'ChildWorkflowExecutionTimedOut',
                'eventTimestamp':dt4,
                'childWorkflowExecutionTimedOutEventAttributes':
                    {'workflowExecution':{'workflowId':'wid'}}}

        child_workflow_completed_event = {'eventId':3,
                'eventType':'ChildWorkflowExecutionCompleted',
                'eventTimestamp':dt3,
                'childWorkflowExecutionCompletedEventAttributes':
                    {'workflowExecution':{'workflowId':'wid'}}}

        child_workflow_failed_event = {'eventId':2,
                'eventType':'ChildWorkflowExecutionFailed',
                'eventTimestamp':dt2,
                'childWorkflowExecutionFailedEventAttributes':
                    {'workflowExecution':{'workflowId':'wid'}}}

        child_workflow_initiated_event = {'eventId':1,
                   'eventType':'StartChildWorkflowExecutionInitiated',
                   'eventTimestamp':dt1,
                   'startChildWorkflowExecutionInitiatedEventAttributes':{'workflowId':'wid'}}

        events = [child_workflow_timed_out_event,
                  child_workflow_completed_event,
                  child_workflow_failed_event,
                  child_workflow_initiated_event]
        empty_response['events'] = events 
        h = floto.History(domain='d', task_list='tl', response=empty_response)
        assert h.get_number_child_workflow_failures('wid') == 1

    def test_get_workflow_input(self, history):
        assert history.get_workflow_input() == 'workflow_input' 

    def test_get_workflow_input_page2(self, mocker, page1_response, page2_response):
        swf_mock = SwfMock()
        swf_mock.pages['page2'] = page2_response
        mocker.patch('floto.api.Swf.client', new_callable=PropertyMock, return_value=swf_mock)
        h = floto.History(domain='d', task_list='tl', response=page1_response)
        assert h.get_workflow_input() == 'workflow_input' 

    def test_get_workflow_input_without_input(self, empty_response, dt1):
        events = [{'eventId':1,
                   'eventTimestamp':dt1,
                   'eventType':'WorkflowExecutionStarted',
                   'workflowExecutionStartedEventAttributes':{}}]
        empty_response['events'] = events
        h = floto.History('d', 'tl', empty_response)
        assert h.get_workflow_input() == {}

    def testget_event_attributes(self, history):
        event = {'eventId':1,
                 'eventType':'ActivityTaskScheduled',
                 'eventTimestamp':dt1,
                 'activityTaskScheduledEventAttributes':{'activityId':'a_id'}}
        attributes = history.get_event_attributes(event)
        assert attributes == {'activityId':'a_id'}

    def test_get_result_completed_activity(self, dt1, dt2, empty_response):
        activity_task_completed_event = {'eventId':2,
                'eventType':'ActivityTaskCompleted',
                'eventTimestamp':dt2,
                'activityTaskCompletedEventAttributes':{'scheduledEventId':1,
                                                        'result':'{"foo":"bar"}'}}

        activity_task_scheduled_event = {'eventId':1,
                   'eventType':'ActivityTaskScheduled',
                   'eventTimestamp':dt1,
                   'activityTaskScheduledEventAttributes':{'activityId':'a_id'}}
        empty_response['events'] = [activity_task_completed_event, activity_task_scheduled_event]
        h = floto.History(domain='d', task_list='tl', response=empty_response)
        task = floto.specs.task.ActivityTask(domain='d', name='a', version='1', activity_id='a_id')
        assert h.get_result_completed_activity(task) == {'foo':'bar'}

    def test_get_result_completed_activity_wo_result(self, dt1, dt2, empty_response):
        activity_task_completed_event = {'eventId':2,
                'eventType':'ActivityTaskCompleted',
                'eventTimestamp':dt2,
                'activityTaskCompletedEventAttributes':{'scheduledEventId':1}}

        activity_task_scheduled_event = {'eventId':1,
                   'eventType':'ActivityTaskScheduled',
                   'eventTimestamp':dt1,
                   'activityTaskScheduledEventAttributes':{'activityId':'a_id'}}
        empty_response['events'] = [activity_task_completed_event, activity_task_scheduled_event]
        h = floto.History(domain='d', task_list='tl', response=empty_response)
        task = floto.specs.task.ActivityTask(domain='d', name='a', version='1', activity_id='a_id')
        assert not h.get_result_completed_activity(task)

    def test_get_result_completed_activity_rescheduled(self, dt1, dt2, dt3, page1_response, 
            page2_response, mocker):

        activity_task_completed_event = {'eventId':3,
                'eventType':'ActivityTaskCompleted',
                'eventTimestamp':dt3,
                'activityTaskCompletedEventAttributes':{'scheduledEventId':1,
                                                        'result':'{"foo":"bar"}'}}
        decision_completed = {'eventId':2,
                'eventType':'DecisionTaskCompleted',
                'eventTimestamp':dt2}

        activity_task_scheduled_event = {'eventId':1,
                   'eventType':'ActivityTaskScheduled',
                   'eventTimestamp':dt1,
                   'activityTaskScheduledEventAttributes':{'activityId':'a_id'}}

        page1_response['previousStartedEventId'] = 2
        page1_response['events'] = [activity_task_completed_event, decision_completed]
        page2_response['events'] = [activity_task_scheduled_event]

        swf_mock = SwfMock()
        swf_mock.pages['page2'] = page2_response
        mocker.patch('floto.api.Swf.client', new_callable=PropertyMock, return_value=swf_mock)

        h = floto.History(domain='d', task_list='tl', response=page1_response)
        task = floto.specs.task.ActivityTask(domain='d', name='a', version='1', activity_id='a_id')
        assert h.get_result_completed_activity(task) == {'foo':'bar'} 

    @pytest.mark.parametrize('events,result',
            [([{'eventType':'type_a'}], [{'eventType':'type_a'}]),
             ([{'eventType':'type_b'}], [{'eventType':'type_b'}]),
             ([{'eventType':'type_c'}], [])])
    def test_filter_events_by_type(self, history, events, result):
        types = ['type_a', 'type_b']
        assert history._filter_events_by_type(events, types) == result


