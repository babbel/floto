import pytest
import json
from floto.decisions import ScheduleActivityTask
from floto.api import ActivityType

@pytest.fixture(scope='module')
def activity_type():
    return ActivityType(name='activity_type_name', version='activity_type_version')

class TestScheduleActivityTask():
    def test_get_decision(self):
        activity_type = ActivityType(name='at', version='1')
        d = ScheduleActivityTask(activity_id='do_it', activity_type=activity_type)
        decision_dict = d.get_decision()
        decision_attributes = decision_dict['scheduleActivityTaskDecisionAttributes']
        assert decision_dict['decisionType'] == 'ScheduleActivityTask'
        assert decision_attributes['activityType']['name'] == 'at'
        assert decision_attributes['activityType']['version'] == '1'
        assert decision_attributes['activityId'] == d.activity_id

    def test_decision_attribute_default_activity_id(self):
        activity_type = ActivityType(name='at', version='1')
        d = ScheduleActivityTask(activity_type=activity_type)
        activity_id = activity_type.name + '_' + activity_type.version
        assert d.decision_attributes()['activityId'] == activity_id 

    def test_path_in_dictionary(self):
        dictionary = {'key':'value'}
        d = ScheduleActivityTask()
        assert d.path_in_dictionary(dictionary, 'key')
        assert not d.path_in_dictionary(dictionary, 'key2')

    def test_path_in_dictionary_several_keys(self):
        dictionary = {'key':{'key2':'val'}}
        d = ScheduleActivityTask()
        assert d.path_in_dictionary(dictionary, 'key.key2')

    def test_path_in_dictionary_several_keys(self):
        dictionary = {'key':{'key2':'val'}}
        d = ScheduleActivityTask()
        assert d.path_in_dictionary(dictionary, 'key.key2')

    def test_assert_required_fields_raises(self):
        dictionary = {'key':'val'}
        d = ScheduleActivityTask()
        with pytest.raises(KeyError) as e:
            d.assert_required_fields(dictionary, ['key','key.key2'])

    def test_assert_required_fields_raises_witout_value(self):
        dictionary = {'key':None}
        d = ScheduleActivityTask()
        with pytest.raises(KeyError) as e:
            d.assert_required_fields(dictionary, ['key'])

    def test_assert_required_fields_raises_witout_value_in_sub_dict(self):
        dictionary = {'key':{'key2':None}}
        d = ScheduleActivityTask()
        with pytest.raises(KeyError) as e:
            d.assert_required_fields(dictionary, ['key', 'key.key2'])

    def test_decision_attributes_with_input(self):
        activity_type = ActivityType(name='at', version='1')
        d = ScheduleActivityTask(activity_type=activity_type)
        d.input={'foo':'bar'}
        assert json.loads(d.decision_attributes()['input'])['foo'] == 'bar'

    def test_decision_attributes_with_input_in_init(self):
        activity_type = ActivityType(name='at', version='1')
        d = ScheduleActivityTask(activity_type=activity_type, input={'foo':'bar'})
        assert json.loads(d.decision_attributes()['input'])['foo'] == 'bar'

    def test_task_list_in_decision_attributes(self, activity_type):
        t = ScheduleActivityTask(activity_type=activity_type, task_list='tl')
        d = t._get_decision()['scheduleActivityTaskDecisionAttributes']
        assert d['taskList']['name'] == 'tl'

