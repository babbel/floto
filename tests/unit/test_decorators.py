import pytest
import json
import floto
import floto.decider
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

    def test_compress_generator_result(self, init_response):
        result = {'foo':'bar'}
        floto.decorators.COMPRESS_GENERATOR_RESULT = True
        z = floto.decorators.compress_generator_result(result)
        floto.decorators.COMPRESS_GENERATOR_RESULT = False

        task = floto.specs.task.ActivityTask(name='activity1', version='v1')
        b = floto.decider.DecisionBuilder([task], 'floto_activities')
        b.history = floto.History('d', 'tl', init_response) 

        uncompressed = b._decompress_result(z)
        assert result == json.loads(uncompressed)

    def test_compress_generator_result_wo_compression(self):
        result = {'foo':'bar'}
        z = floto.decorators.compress_generator_result(result)
        assert z == result

