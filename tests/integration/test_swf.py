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

from activity_worker import ActivityWorkerProcess
from test_helper import get_activity_result

worker1 = ActivityWorkerProcess(domain='floto_test', task_list='floto_activities')
worker2 = ActivityWorkerProcess(domain='floto_test', task_list='floto_activities')
worker1.start()
worker2.start()


def run_01():
    # Single task with context
    result = test_01()
    result = get_activity_result(result, 'activity1', 'v5')
    assert result['workflow'] == {'foo': 'bar'}
    assert result['status'] == 'finished'


def run_02():
    # Single task without context
    result = test_02()
    result = get_activity_result(result, 'activity2', 'v4')
    assert result['status'] == 'finished'


def run_03():
    # Two tasks without dependency, run in parallel if > 1 worker
    result = test_03()
    result1 = get_activity_result(result, 'activity1', 'v5')
    result2 = get_activity_result(result, 'activity2', 'v4')

    assert result1['workflow'] == {'foo': 'bar'}
    assert result1['status'] == 'finished'
    assert result2['status'] == 'finished'


def run_04():
    # Two tasks with 1 -> 3
    result = test_04()
    result3 = get_activity_result(result, 'activity3', 'v2')

    assert result3['activity1']['status'] == 'finished'
    assert result3['activity1']['workflow'] == {'foo': 'bar'}
    assert result3['status'] == 'finished'


def run_05():
    # Failing task with retry strategy, succeeds after retry
    result = test_05()
    result = get_activity_result(result, 'activity_fails_3', 'v2')
    assert result['workflow_input'] == {'foo': 'bar'}
    assert result['status'] == 'finished'


def run_06():
    # Failing task with retry strategy, reaches limit of retries
    details = test_06()
    details = get_activity_result(details, 'activity_fails_2', 'v2')
    assert details == 'Something went wrong'


def run_07():
    # Timeout
    result = test_07()
    result = get_activity_result(result, 'activity2', 'v4')
    assert result['status'] == 'finished'


def run_08():
    # Repeated Workflow
    result = test_08()
    result = get_activity_result(result, 'activity1', 'v5')
    assert result['status'] == 'finished'


def run_09():
    # Repeated Workflow with timer and failing activity with retries
    result = test_09()
    result = get_activity_result(result, 'activity4', 'v2')
    assert [r for r in result.keys() if 'activity1' in r]
    assert [r for r in result.keys() if 'activity2' in r]


def run_10():
    # Activity fails several times due to heartbeat timeout, succeeds after retry
    # Might print warnings
    result = test_10()
    result = get_activity_result(result, 'activity5', 'v1')
    assert result['sleep_time'] == 0


def run_11():
    # Decider times out, succeeds after next decision task
    # Prints a warning due to Decider timeout
    result = test_11()
    result = get_activity_result(result, 'activity1', 'v5')
    assert result['workflow'] == {'foo': 'bar'}
    assert result['status'] == 'finished'


def run_12():
    # run_09 with 2 parallel deciders
    result = test_12()
    result = get_activity_result(result, 'activity4', 'v2')
    assert [r for r in result.keys() if 'activity1' in r]
    assert [r for r in result.keys() if 'activity2' in r]


def run_13():
    # Two parallel deciders, one of them times out
    result = test_13()
    result = get_activity_result(result, 'activity4', 'v2')
    assert [r for r in result.keys() if 'activity1' in r]
    assert [r for r in result.keys() if 'activity2' in r]

def run_14():
    # Simple test with child workflow
    result = test_14()
    print(result)

tests = [run_01, run_02, run_03, run_04, run_05, run_06, run_07, run_08, run_09, run_10, run_11,
         run_12, run_13]

tests = [run_14]
       
try:
    [t() for t in tests]
except (KeyboardInterrupt, SystemExit):
    worker1.terminate()
    worker2.terminate()

print()
print('All workflows finished successfully.')
print('Waiting for workers to finish last poll and terminate.')

worker1.terminate()
worker2.terminate()
