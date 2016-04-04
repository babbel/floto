import pytest
import json
import datetime
import floto.decider
from floto.specs.task import ActivityTask, Timer, ChildWorkflow, Generator
from floto.specs import DeciderSpec
import floto.specs.retry_strategy

@pytest.fixture
def task_1():
    return ActivityTask(domain='d', name='activity1', version='v1', input={'date':1})

@pytest.fixture
def task_2(task_1):
    return ActivityTask(domain='d', name='activity2', version='v1', requires=[task_1.id_])

@pytest.fixture
def generator_task():
    return Generator(domain='d', name='g', version='v1')

@pytest.fixture
def generator_task_2():
    return Generator(domain='d', name='g2', version='v1')

@pytest.fixture
def child_workflow_task():
    args = {'domain': 'd',
            'workflow_type_name':'wft_name',
            'workflow_type_version':'wft_version',
            'task_list':'tl',
            'input':{'path':'filename'}}
    return ChildWorkflow(**args)

@pytest.fixture
def timer():
    return Timer(id_='timer_id', delay_in_seconds=60)

@pytest.fixture
def empty_history(init_response):
    return floto.History(domain='d', task_list='tl', response=init_response)

@pytest.fixture
def dt1():
    return datetime.datetime(2016, 1, 12, hour=1, tzinfo=datetime.timezone.utc)

@pytest.fixture
def builder(task_1, task_2, empty_history):
    b = floto.decider.DecisionBuilder(activity_tasks=[task_1, task_2], 
                                      default_activity_task_list='floto_activities')
    b._set_history(empty_history)
    return b

@pytest.fixture
def builder_with_graph(builder):
    builder._build_execution_graph()
    return builder

class TestDecisionBuilder(object):
    def test_init(self, task_1):
        b = floto.decider.DecisionBuilder(activity_tasks=[task_1], default_activity_task_list='atl')
        assert [t.id_ for t in b.activity_tasks] == [task_1.id_]
        assert b.workflow_fail == False
        assert b.workflow_complete == False
        assert b.default_activity_task_list == 'atl'
        assert b.tasks_by_id == {task_1.id_:task_1}

    def test_get_decisions(self, mocker, builder):
        history = type('History', (object,), {'previous_decision_id':0,
                                              'decision_task_started_event_id': 3})
        mocker.patch('floto.decider.DecisionBuilder._collect_decisions', return_value=['d'])
        assert builder.get_decisions(history) == ['d']
        builder._collect_decisions.assert_called_once_with()

    def test_set_history(self, builder):
        builder._set_history('history')
        assert builder.history == 'history'
        assert builder.decision_input.history == 'history'

    @pytest.mark.parametrize('failed, completed, is_terminate',
            [(False, False, False),
             (False, True, True),
             (True, False, True),
             (True, True, True)])
    def test_is_terminate_workflow(self, failed, completed, is_terminate, builder):
        builder.workflow_fail = failed
        builder.workflow_complete = completed
        assert builder.is_terminate_workflow() == is_terminate

    def test_collect_decisions_after_workflow_start(self, mocker, builder):
        mocker.patch('floto.decider.DecisionBuilder.get_decisions_after_workflow_start', 
                return_value=['d'])
        builder.first_event_id = 0
        decisions = builder._collect_decisions()
        assert decisions == ['d']

    def test_collect_decisions_faulty_events(self, builder, mocker):
        events = {'decision_failed':[], 'faulty':['e'], 'completed':[]}
        mocker.patch('floto.History.get_events_for_decision', return_value=events)
        mocker.patch('floto.decider.DecisionBuilder.get_decisions_faulty_tasks', return_value=[])
        builder._collect_decisions()
        builder.get_decisions_faulty_tasks.assert_called_once_with(['e'])

    def test_collect_decisions_completed_events(self, builder, mocker):
        events = {'decision_failed':[], 'faulty':[], 'completed':['e']}
        mocker.patch('floto.History.get_events_for_decision', return_value=events)
        mocker.patch('floto.decider.DecisionBuilder.get_decisions_after_activity_completion', 
                return_value=[])
        mocker.patch('floto.decider.DecisionBuilder.all_workflow_tasks_finished', 
                return_value=False) 
        builder._collect_decisions()
        builder.get_decisions_after_activity_completion.assert_called_once_with(['e'])

    def test_collect_decisions_completed_events_workflow_finished(self, builder_with_graph, 
            mocker):
        events = {'decision_failed':[], 'faulty':[], 'completed':['e']}
        mocker.patch('floto.History.get_events_for_decision', return_value=events)
        fct ='floto.decider.DecisionInput.collect_results'
        mocker.patch(fct, return_value='result')
        mocker.patch('floto.decider.DecisionBuilder.all_workflow_tasks_finished', 
                return_value=True) 
        d = builder_with_graph._collect_decisions()
        assert builder_with_graph.is_terminate_workflow() == True
        assert isinstance(d[0], floto.decisions.CompleteWorkflowExecution)
        assert len(d) == 1

    def test_collect_decisions_failed_decision(self, builder, mocker):
        events = {'decision_failed':['e'], 'faulty':[], 'completed':[]}
        mocker.patch('floto.History.get_events_for_decision', return_value=events)
        mocker.patch('floto.decider.DecisionBuilder.get_decisions_decision_failed', 
                return_value=[])
        builder._collect_decisions()
        builder.get_decisions_decision_failed.assert_called_once_with(['e'])

    def test_get_decisions_first_decision_task(self, builder, empty_history):
        empty_history.first_decision_task = lambda: True
        builder.get_decisions_after_workflow_start = lambda: ['good_decision']
        decisions = builder.get_decisions(empty_history)
        assert decisions == ['good_decision']

    def test_get_decisions_faulty_tasks(self, mocker, task_1, builder_with_graph):
        task_1.retry_strategy = floto.specs.retry_strategy.InstantRetry(retries=1)
        task_1.id_ = 'a_id'
        scheduled_event = {'eventType':'ActivityTaskScheduled',
                           'activityTaskScheduledEventAttributes':{}}
        mocker.patch('floto.History.get_id_task_event', return_value='a_id')
        mocker.patch('floto.History.get_number_activity_task_failures', return_value=1)
        mocker.patch('floto.History.get_event_by_task_id_and_type', return_value=scheduled_event)
        mocker.patch('floto.decider.DecisionBuilder.get_decision_schedule_activity', 
                return_value='d')
        builder_with_graph.tasks_by_id = {'a_id':task_1}
        d = builder_with_graph.get_decisions_faulty_tasks(task_events=['te'])
        builder_with_graph.get_decision_schedule_activity.assert_called_once_with(task=task_1)
        assert d == ['d']
        assert builder_with_graph.is_terminate_workflow() == False

    def test_get_decisions_faulty_tasks_retry_limit_reached(self, mocker, task_1, 
            builder_with_graph, dt1):
        task_1.retry_strategy = floto.specs.retry_strategy.InstantRetry(retries=1)
        task_1.id_ = 'a_id'
        scheduled_event = {'eventType':'ActivityTaskScheduled',
                           'activityTaskScheduledEventAttributes':{}}

        mocker.patch('floto.History.get_id_activity_task_event', return_value='a_id')
        mocker.patch('floto.History.get_number_activity_task_failures', return_value=2)
        builder_with_graph.tasks_by_id = {'a_id':task_1}

        task_failed_event = {'eventId':4,
                             'eventType':'ActivityTaskFailed',
                             'eventTimestamp':dt1,
                             'activityTaskFailedEventAttributes':{'scheduledEventId':3, 
                                                                  'details':'Error'}}
        d = builder_with_graph.get_decisions_faulty_tasks(task_events=[task_failed_event])
        assert len(d) == 1
        assert isinstance(d[0], floto.decisions.FailWorkflowExecution)
        assert builder_with_graph.is_terminate_workflow() == True
        assert d[0].details['a_id'] == 'Error'
        assert d[0].reason == 'task_retry_limit_reached'

    def test_get_decisions_faulty_tasks_without_retry_strategy(self, mocker, task_1, 
            builder_with_graph, dt1):
        task_1.id_ = 'a_id'
        scheduled_event = {'eventType':'ActivityTaskScheduled',
                           'activityTaskScheduledEventAttributes':{}}
        mocker.patch('floto.History.get_id_activity_task_event', return_value='a_id')
        mocker.patch('floto.History.get_number_activity_task_failures', return_value=2)

        builder_with_graph.tasks_by_id = {'a_id':task_1}

        task_failed_event = {'eventId':4,
                             'eventType':'ActivityTaskFailed',
                             'eventTimestamp':dt1,
                             'activityTaskFailedEventAttributes':{'scheduledEventId':3, 
                                                                  'details':'Error'}}
        d = builder_with_graph.get_decisions_faulty_tasks(task_events=[task_failed_event])
        assert len(d) == 1
        assert isinstance(d[0], floto.decisions.FailWorkflowExecution)
        assert builder_with_graph.is_terminate_workflow() == True
        assert d[0].details['a_id'] == 'Error'
        assert d[0].reason == 'task_failed'
    
    def test_get_decisions_faulty_tasks_w_input(self, mocker, task_1, builder):
        task_1.retry_strategy = floto.specs.retry_strategy.InstantRetry(retries=1)
        task_1.id_ = 'a_id'
        builder.activity_tasks = [task_1]
        builder.tasks_by_id = {task_1.id_:task_1}
        builder._build_execution_graph()

        mocker.patch('floto.History.get_id_task_event', return_value='a_id')
        mocker.patch('floto.History.get_number_activity_task_failures', return_value=1)

        d = builder.get_decisions_faulty_tasks(task_events=['te'])
        assert d[0].input['activity_task'] == {'date':1}

    def test_get_decisions_faulty_tasks_early_exit(self, builder):
        builder.workflow_fail = True
        d = builder.get_decisions_faulty_tasks(['e'])
        assert d == []

    def test_get_decisions_after_workflow_start(self, builder_with_graph):
        d = builder_with_graph.get_decisions_after_workflow_start()
        assert len(d) == 1
        assert isinstance(d[0], floto.decisions.ScheduleActivityTask)
        assert 'activity1:v1' in d[0].activity_id

    def test_get_decision_after_workflow_start_with_timer(self, timer):
        builder = floto.decider.DecisionBuilder(activity_tasks=[timer], 
                                          default_activity_task_list='floto_activities')
        builder._build_execution_graph()
        d = builder.get_decisions_after_workflow_start()
        assert len(d) == 1
        assert isinstance(d[0], floto.decisions.StartTimer)
        assert d[0].start_to_fire_timeout == 60

    def test_get_decisions_after_workflow_start_with_child_workflow(self, mocker, 
            child_workflow_task, empty_history):

        builder = floto.decider.DecisionBuilder(activity_tasks=[child_workflow_task], 
                                          default_activity_task_list='floto_activities')
        builder._build_execution_graph()
        builder._set_history(empty_history)
        d = builder.get_decisions_after_workflow_start()
        assert len(d) == 1
        assert isinstance(d[0], floto.decisions.StartChildWorkflowExecution)
        assert d[0].workflow_type['name'] == 'wft_name'

    def test_get_decisions_after_workflow_start_w_wf_input(self, builder, init_response, task_1):
        dt1 = datetime.datetime(2016, 1, 12, hour=1, tzinfo=datetime.timezone.utc)
        events = [{'eventId':1,
                   'eventType':'WorkflowExecutionStarted',
                   'eventTimestamp':dt1,
                   'workflowExecutionStartedEventAttributes':{'input':{'foo':'bar'}}}]
        init_response['events'] = events
        builder._set_history(floto.History(domain='d', task_list='tl', response=init_response))
        builder._build_execution_graph()
        d = builder.get_decisions_after_workflow_start()
        assert len(d) == 1
        assert isinstance(d[0], floto.decisions.ScheduleActivityTask)
        assert 'activity1:v1' in d[0].activity_id
        assert d[0].input['activity_task'] == task_1.input 
        assert d[0].input['workflow'] == {'foo':'bar'} 
    
    def test_get_decisions_decision_failed(self, builder, mocker):
        mocker.patch('floto.History.get_id_previous_started', return_value=1)
        mocker.patch('floto.decider.DecisionBuilder._collect_decisions', return_value=['d'])
        failed = {'eventId':3,
                  'eventType':'DecisionTaskTimedOut',
                  'decisionTaskTimedOutEventAttributes':{'startedEventId':2}}
        d = builder.get_decisions_decision_failed([failed])
        assert d == ['d']
        
    def test_get_decisions_after_successful_workflow_execution(self, builder, mocker, task_2):
        mocker.patch('floto.decider.DecisionInput.collect_results', return_value='result')
        builder._build_execution_graph()
        d = builder.get_decisions_after_successful_workflow_execution()
        assert isinstance(d[0], floto.decisions.CompleteWorkflowExecution)
        assert d[0].result == 'result'
        assert builder.is_terminate_workflow() == True
        builder.decision_input.collect_results.assert_called_with([task_2])

    def test_get_decision_schedule_activity_with_activity_task(self, mocker, builder_with_graph, 
            task_1, empty_history):
        builder_with_graph._set_history(empty_history)
        d = builder_with_graph.get_decision_schedule_activity(task=task_1)
        assert isinstance(d, floto.decisions.ScheduleActivityTask)
        assert d.input['activity_task'] == {'date':1} 
        assert d.input['workflow'] == 'workflow_input' 
        assert d.activity_id == task_1.id_ 

    def test_get_decision_schedule_activity_with_child_workflow(self, mocker, empty_history,
            child_workflow_task):
        builder = floto.decider.DecisionBuilder(activity_tasks=[child_workflow_task], 
                                          default_activity_task_list='floto_activities')
        builder._build_execution_graph()
        builder._set_history(empty_history)
        
        d = builder.get_decision_schedule_activity(task=child_workflow_task)

        assert isinstance(d, floto.decisions.StartChildWorkflowExecution)
        input_ = json.loads(d._get_attribute('input'))
        assert input_['child_workflow_task'] == {'path':'filename'} 
        assert 'wft_name' in d.workflow_id

    def test_get_decision_schedule_activity_with_timer(self, builder_with_graph, mocker):
        task = floto.specs.task.Timer(id_='t_id', delay_in_seconds=10)
        mocker.patch('floto.decider.ExecutionGraph.get_requires')
        d = builder_with_graph.get_decision_schedule_activity(task=task)
        assert isinstance(d, floto.decisions.StartTimer)
        assert d.start_to_fire_timeout == 10

    def test_get_decision_after_activity_completion(self, mocker, builder_with_graph, task_1):
        mocker.patch('floto.History.get_id_activity_task_event', return_value=task_1.id_)
        mocker.patch('floto.History.is_task_completed', return_value=True)
        events = [{'eventType':'ActivityTaskCompleted'}]
        d = builder_with_graph.get_decisions_after_activity_completion(events)
        assert isinstance(d[0], floto.decisions.ScheduleActivityTask)
        assert 'activity2:v1' in d[0].activity_id

    def test_outgoing_nodes_completed(self, mocker, builder_with_graph, task_2):
        mocker.patch('floto.History.is_task_completed', return_value=True)
        assert builder_with_graph.outgoing_nodes_completed()
        builder_with_graph.history.is_task_completed.assert_called_once_with(task_2)

    def test_outgoing_nodes_not_completed(self, mocker, builder_with_graph, task_2):
        mocker.patch('floto.History.is_task_completed', return_value=False)
        assert not builder_with_graph.outgoing_nodes_completed()
        builder_with_graph.history.is_task_completed.assert_called_once_with(task_2)
        
    def test_get_task_to_be_scheduled(self, empty_history):
        t_a = floto.specs.task.ActivityTask(domain='d', name='a', version='v', id_='a')
        t_b = floto.specs.task.ActivityTask(domain='d', name='b', version='v', id_='b')
        t_c = floto.specs.task.ActivityTask(domain='d', name='c', version='v', id_='c', 
                                          requires=[t_a.id_,t_b.id_])
        t_d = floto.specs.task.ActivityTask(domain='d', name='d', version='v', id_='d', 
                                          requires=[t_b.id_])
        b = floto.decider.DecisionBuilder(activity_tasks=[t_a,t_b,t_c,t_d], 
                default_activity_task_list='tl')
        b._set_history(empty_history)
        b._build_execution_graph()
        b.history.is_task_completed = lambda x: {t_a:True, t_b:True, t_c:False, t_d:False}[x]
        tasks = b.get_tasks_to_be_scheduled(['a', 'b'])
        assert set([t.id_ for t in tasks]) == set(['c', 'd'])

    def test_get_decision_schedule_activity_task(self, builder):
        at = floto.specs.task.ActivityTask(domain='d', name='at_name', version='at_version', 
                id_='id')
        d = builder.get_decision_schedule_activity_task(activity_task=at)
        assert isinstance(d, floto.decisions.ScheduleActivityTask)
        assert d.activity_type.name == at.name
        assert d.activity_type.version == at.version
        assert d.activity_id == at.id_
        assert d.task_list == builder.default_activity_task_list

    def test_get_decision_schedule_activity_task(self, builder):
        at = floto.specs.task.ActivityTask(domain='d', name='at_name', version='at_version', id_='id',
                task_list='tl_of_task')
        d = builder.get_decision_schedule_activity_task(activity_task=at)
        assert isinstance(d, floto.decisions.ScheduleActivityTask)
        assert d.task_list == at.task_list 

    def test_get_decision_start_timer(self, builder):
        timer_task = floto.specs.task.Timer(id_='t_id', delay_in_seconds=60)
        decision = builder.get_decision_start_timer(timer_task=timer_task)
        assert decision.start_to_fire_timeout == 60
        assert decision.timer_id == 't_id'

    def test_get_decision_start_child_workflow(self, builder):
        cw_task = floto.specs.task.ChildWorkflow(domain='d', workflow_type_name='wft_name',
                workflow_type_version='v1')
        d = builder.get_decision_start_child_workflow_execution(child_workflow_task=cw_task)
        assert isinstance(d, floto.decisions.StartChildWorkflowExecution)
        assert d.workflow_type['name'] == 'wft_name'
        assert d.workflow_type['version'] == 'v1'

    def test_get_decision_start_child_workflow_w_attributes(self, builder):
        cw_task = floto.specs.task.ChildWorkflow(domain='d', workflow_type_name='wft_name',
                workflow_type_version='v1',
                task_list='tl')
        d = builder.get_decision_start_child_workflow_execution(child_workflow_task=cw_task, 
                input_={'foo':'bar'})
        assert d.task_list['name'] == 'tl'
        assert json.loads(d._get_attribute('input')) == {'foo':'bar'}

    def test_all_workflow_tasks_finished_with_generator(self, builder, mocker):
        mocker.patch('floto.decider.DecisionBuilder.completed_contain_generator', 
                return_value=True)
        assert not builder.all_workflow_tasks_finished(['t'])

    def test_all_workflow_tasks_finished_depending_tasks(self, builder_with_graph, mocker):
        mocker.patch('floto.decider.DecisionBuilder.completed_contain_generator', 
                return_value=False)
        mocker.patch('floto.decider.DecisionBuilder.completed_have_depending_tasks', 
                return_value=True)
        assert not builder_with_graph.all_workflow_tasks_finished(['t'])

    def test_all_workflow_tasks_finished_open_tasks(self, builder_with_graph, mocker):
        mocker.patch('floto.decider.DecisionBuilder.completed_contain_generator', 
                return_value=False)
        mocker.patch('floto.decider.DecisionBuilder.completed_have_depending_tasks', 
                return_value=False)
        assert not builder_with_graph.all_workflow_tasks_finished(['t'])

    def test_all_workflow_tasks_finished_not_completed(self, builder_with_graph, mocker):
        mocker.patch('floto.decider.DecisionBuilder.completed_contain_generator', 
                return_value=False)
        mocker.patch('floto.decider.DecisionBuilder.completed_have_depending_tasks', 
                return_value=False)
        mocker.patch('floto.decider.DecisionBuilder.outgoing_nodes_completed', 
                return_value=False)
        assert not builder_with_graph.all_workflow_tasks_finished(['t'])

    def test_all_workflow_tasks_finished(self, builder_with_graph, mocker):
        mocker.patch('floto.decider.DecisionBuilder.completed_contain_generator', 
                return_value=False)
        mocker.patch('floto.decider.DecisionBuilder.completed_have_depending_tasks', 
                return_value=False)
        mocker.patch('floto.decider.DecisionBuilder.outgoing_nodes_completed',
                return_value=True)
        assert builder_with_graph.all_workflow_tasks_finished(['t'])
        
    def test_update_execution_graph(self, mocker, generator_task, empty_history):
        b = floto.decider.DecisionBuilder(activity_tasks=[generator_task], 
                                          default_activity_task_list='floto_activities')
        b._set_history(empty_history)
        b.last_event_id = 0
        b._build_execution_graph()

        task = floto.specs.task.ActivityTask(domain='d', name='n', version='v1')
        generator_result = [task.serializable()]
        
        mocker.patch('floto.History.get_result_completed_activity', return_value=generator_result)
        
        b._update_execution_graph(generator_task)
        assert b.tasks_by_id[task.id_]

    #def test_update_execution_graph_with_compression(self, mocker, builder):
        #result = json.dumps('r')
        #mocker.patch('floto.decider.DecisionBuilder._decompress_result', return_value=result)
        #mocker.patch('floto.History.get_result_completed_activity', return_value='gr')
        #mocker.patch('floto.decider.ExecutionGraph.update')
        #builder._decompress_generator_result = True
        #builder._update_execution_graph('g')
        #builder._decompress_result.assert_called_once_with('gr')

    #def test_decompress_result(self, mocker, builder):
        #tasks = ['t']
        #generator_result = json.loads(floto.specs.JSONEncoder.dump_object(tasks))
        #floto.decorators.COMPRESS_GENERATOR_RESULT=True
        #compressed_result = floto.decorators.compress_generator_result(generator_result)
        #floto.decorators.COMPRESS_GENERATOR_RESULT=False

        #result = builder._decompress_result(compressed_result)
        #assert result == ['t']

    def test_get_generator(self, builder, mocker):
        mocker.patch('floto.History.get_id_task_event', return_value='g_id')
        generator = floto.specs.task.Generator(domain='d',name='generator', version='v1')
        builder.tasks_by_id = {'g_id':generator}
        generator = builder._get_generator({'eventType':'ActivityTaskCompleted'})
        assert generator.name == 'generator'

    def test_get_no_generator_wrong_type(self, builder, mocker):
        generator = builder._get_generator({'eventType':'TimerFired'})
        assert not generator

    def test_get_no_generator(self, builder, mocker):
        mocker.patch('floto.History.get_id_task_event', return_value='a_id')
        activity_task = floto.specs.task.ActivityTask(domain=' d',name='activity', version='v1')
        builder.tasks_by_id = {'a_id':activity_task}
        generator = builder._get_generator({'eventType':'ActivityTaskCompleted'})
        assert not generator 

    def test_build_execution_graph_wo_generator(self, builder, task_1, task_2):
        builder._build_execution_graph()
        assert builder.execution_graph.outgoing_vertices.keys() == set([task_1.id_, task_2.id_])

    def test_build_execution_graph_w_generator(self, task_1, generator_task, mocker, 
            empty_history):

        completed = [{'eventType':'ActivityTaskCompleted', 'eventId':1},
                {'eventType':'ActivityTaskCompleted', 'eventId':2}] 
        mocker.patch('floto.History.get_events_for_decision', return_value={'completed':completed})
        mocker.patch('floto.decider.DecisionBuilder._update_execution_graph_with_completed_events')

        b = floto.decider.DecisionBuilder(activity_tasks=[task_1, generator_task],
                                          default_activity_task_list='floto_activities')
        b.last_event_id = 3
        b.history = empty_history
        b._build_execution_graph()

        b.history.get_events_for_decision.assert_called_once_with(1,3)
        completed_arg = b._update_execution_graph_with_completed_events.call_args[0][0]
        assert [e['eventId'] for e in completed_arg] == [1,2]





