import pytest
from floto.api import SwfType

class TestSwfType:
    def test_init_with_parameters(self):
        t = SwfType(domain='d')
        assert t.domain == 'd'

    def test_snake_case_parameter(self):
        t = SwfType(default_task_list='42')
        assert t.default_task_list == '42'

    def test_camel_case_parameter(self):
        t = SwfType(defaultTaskList='42')
        assert t.default_task_list == '42'

    @pytest.mark.parametrize(('prop', 'value'), 
            [('default_task_start_to_close_timeout', str(60*60*6)),
             ('default_task_list', 'default'),
             ('default_task_priority', '0')])
    def test_init_default_properties(self, prop, value):
        t = SwfType()
        assert getattr(t, prop) == value 

    def test_set_properties_w_value(self):
        w = SwfType()
        w._set_attributes(['my_field'], {'my_field':None}, {'my_field':'my_value'})
        assert w._my_field == 'my_value'

    def test_set_properties_wo_value(self):
        w = SwfType()
        w._set_attributes(['my_field'], {}, {})
        assert w._my_field is None

    def test_set_properties_w_user_value(self):
        w = SwfType()
        w._set_attributes(['my_field'], {'my_field':'42'}, {})
        assert w._my_field == '42' 

    def test_set_properties_w_default_value(self):
        w = SwfType()
        w._set_attributes(['my_field'], {}, {'my_field':'42'})
        assert w._my_field == '42' 

    def test_set_properties_w_camel_case_attribute(self):
        w = SwfType()
        w._set_attributes(['myField'], {}, {'myField':'42'})
        assert w._my_field == '42' 

    def test_set_properties_w_camel_case_user_value(self):
        w = SwfType()
        w._set_attributes(['myField'], {'myField':'42'}, {})
        assert w._my_field == '42' 

    @pytest.mark.parametrize('key,value',[
        ('name','my_name'),
        ('defaultTaskList',{'name':'my_tl'})
        ])
    def test_get_properties(self, key, value):
        w = SwfType(name='my_name', domain='my_domain', version='my_version', default_task_list='my_tl')
        properties = w._get_properties()
        assert properties[key] == value

    @pytest.mark.parametrize('snake, camel',[
        ('my_var', 'myVar'),
        ('my', 'my'),
        ('my_long_var', 'myLongVar')])
    def test__camel_case(self, snake, camel):
        assert SwfType()._camel_case(snake) == camel

