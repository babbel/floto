import json

from deciders.test_01 import test_01
from deciders.test_02 import test_02
from deciders.test_03 import test_03
from deciders.test_04 import test_04
from deciders.test_05 import test_05
from deciders.test_06 import test_06
from deciders.test_07 import test_07
from deciders.test_08 import test_08
from deciders.test_09 import test_09
from deciders.test_10 import test_10
from deciders.test_11 import test_11
from deciders.test_12 import test_12
from deciders.test_13 import test_13
from deciders.test_14 import test_14
from deciders.test_15 import test_15
from deciders.test_16 import test_16
from deciders.test_17 import test_17
from test_helper import get_activity_result, docprint, print_result, print_details

from activity_worker import ActivityWorkerProcess

worker1 = ActivityWorkerProcess(domain='floto_test', task_list='floto_activities')
worker2 = ActivityWorkerProcess(domain='floto_test', task_list='floto_activities')
worker1.start()
worker2.start()

@docprint
def run_01():
    """
    Test 01
    Single task with context

    """
    result = test_01()
    result_activity_1 = get_activity_result(result, 'activity1', 'v5')
    print_result(result)
    assert result_activity_1['workflow'] == {'foo': 'bar'}
    assert result_activity_1['status'] == 'finished'


@docprint
def run_02():
    """Test 02
    Single task without context

    """
    result = test_02()
    result_activity_2 = get_activity_result(result, 'activity2', 'v4')
    print_result(result)
    assert result_activity_2['status'] == 'finished'


@docprint
def run_03():
    """Test 03
    Two tasks without dependency, run in parallel if > 1 worker

    """
    result = test_03()
    result1 = get_activity_result(result, 'activity1', 'v5')
    result2 = get_activity_result(result, 'activity2', 'v4')
    print_result(result)

    assert result1['workflow'] == {'foo': 'bar'}
    assert result1['status'] == 'finished'
    assert result2['status'] == 'finished'


@docprint
def run_04():
    """Test 04
    Two tasks with 1 -> 3

    """
    result = test_04()
    result3 = get_activity_result(result, 'activity3', 'v2')
    print_result(result)

    assert result3['activity1']['status'] == 'finished'
    assert result3['activity1']['workflow'] == {'foo': 'bar'}
    assert result3['status'] == 'finished'


@docprint
def run_05():
    """Test 05
    Failing task with retry strategy, succeeds after retry

    """
    result = test_05()
    result3 = get_activity_result(result, 'activity_fails_3', 'v2')
    print_result(result)
    assert result3['workflow_input'] == {'foo': 'bar'}
    assert result3['status'] == 'finished'


@docprint
def run_06():
    """Test 06
    Failing task with retry strategy, reaches limit of retries

    """
    details = test_06()
    details2 = get_activity_result(details, 'activity_fails_2', 'v2')
    print_details(details)
    assert details2 == 'Something went wrong'


@docprint
def run_07():
    """Test 07
    Timeout

    """
    result = test_07()
    result2 = get_activity_result(result, 'activity2', 'v4')
    print_result(result)
    assert result2['status'] == 'finished'


@docprint
def run_08():
    """Test 08
    Repeated Workflow

    """
    result = test_08()
    print_result(result)
    result1 = get_activity_result(result, 'activity1', 'v5')
    assert result1['status'] == 'finished'


@docprint
def run_09():
    """Test 09
    Repeated Workflow with timer and failing activity with retries

    """
    result = test_09()
    print_result(result)
    result4 = get_activity_result(result, 'activity4', 'v2')
    assert [r for r in result4.keys() if 'activity1' in r]
    assert [r for r in result4.keys() if 'activity2' in r]


@docprint
def run_10():
    """Test 10
    Testing heartbeat: Heartbeat(20s) < execution time of activity5_v2 (30s)
    """

    result = test_10()
    result = get_activity_result(result, 'activity5', 'v2')
    print('Result: ' + json.dumps(result) + '\n')
    assert result['status'] == 'finished'


@docprint
def run_11():
    """Test 11
    Decider times out, succeeds after next decision task
    Prints a warning due to Decider timeout
    """

    result = test_11()
    result = get_activity_result(result, 'activity1', 'v5')
    print('Result: ' + json.dumps(result) + '\n')
    assert result['workflow'] == {'foo': 'bar'}
    assert result['status'] == 'finished'


@docprint
def run_12():
    """Test 12
    run_09 with 2 parallel deciders
    """

    result = test_12()
    result = get_activity_result(result, 'activity4', 'v2')
    print('Result: ' + json.dumps(result) + '\n')
    assert [r for r in result.keys() if 'activity1' in r]
    assert [r for r in result.keys() if 'activity2' in r]


@docprint
def run_13():
    """Test 13
    Two parallel deciders, one of them times out

    """
    result = test_13()
    print_result(result)
    result4 = get_activity_result(result, 'activity4', 'v2')
    assert [r for r in result4.keys() if 'activity1' in r]
    assert [r for r in result4.keys() if 'activity2' in r]


@docprint
def run_14():
    """Test 14
    Simple test with child workflow

    """
    result = test_14()
    print_result(result)
    result_cw = get_activity_result(result, 'test_child_workflow', 'v2')
    assert [r for r in result_cw.keys() if 'activity2' in r]


@docprint
def run_15():
    """Test 15
    Workflow schedules a child workflow.

    """
    result = test_15()
    print_result(result)
    result_child_workflow = get_activity_result(result, 'test_child_workflow', 'v2')
    result_activity = get_activity_result(result_child_workflow, 'activity1', 'v5')
    assert result_activity['status'] == 'finished'


@docprint
def run_16():
    """Test 16
    Failing Task in ChildWorkflow

    """
    result = test_16()
    print_result(result)
    result_child_workflow = get_activity_result(result, 'test_child_workflow', 'v2')
    result_activity = get_activity_result(result_child_workflow, 'activity_fails_2', 'v2')
    assert result_activity == 'Something went wrong'

@docprint
def run_17():
    """Test 17
    Activity generates tasks.
    """
    result = test_17()
    print_result(result)
    result_activity_6 = get_activity_result(result, 'activity6', 'v1')
    assert set(result_activity_6) == set(['a.in', 'b.in'])

tests = [run_01, run_02, run_03, run_04, run_05, run_06, run_07, run_08, run_09, run_10, run_11,
         run_12, run_13, run_14, run_15, run_16, run_17]

try:
    [t() for t in tests]
except (KeyboardInterrupt, SystemExit):
    worker1.terminate()
    worker2.terminate()

print()
print('All workflows finished successfully.')

worker1.terminate()
worker2.terminate()
