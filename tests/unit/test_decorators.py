import pytest
import floto
import floto.specs.task

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

    def test_generator(self):
        task = floto.specs.task.ActivityTask(name='n', version='v')
        @floto.generator(name='my_generator', version='v1')
        def my_generator(context):
            return [task]
        assert floto.ACTIVITY_FUNCTIONS['my_generator:v1']('context') == [task]

    def test_generator_raises(self):
        @floto.generator(name='my_generator', version='v2')
        def my_generator(context):
            return 'result' 
        with pytest.raises(ValueError):
            floto.ACTIVITY_FUNCTIONS['my_generator:v2']('context')
