import pytest
import floto

class TestDecorators():
    def test_activity(self):
        @floto.activity(name='my_func', version='v1')
        def my_activity():
            return 'result'
        assert floto.ACTIVITY_FUNCTIONS['my_func:v1']() == 'result'

    def test_activity_with_context(self):
        @floto.activity(name='my_func', version='v2')
        def my_activity_with_context(context):
            return context 
        assert floto.ACTIVITY_FUNCTIONS['my_func:v2']({'foo':'bar'}) == {'foo':'bar'}

        
