import pytest
import json
import floto
import floto.decider
import floto.specs.task

class TestDecorators():
    def test_activity(self):
        @floto.activity(domain='d', name='my_func', version='v1')
        def my_activity():
            return 'result'
        assert floto.ACTIVITY_FUNCTIONS['my_func:v1:d']() == 'result'

    def test_activity_with_context(self):
        @floto.activity(domain='d', name='my_func', version='v2')
        def my_activity_with_context(context):
            return context 
        assert floto.ACTIVITY_FUNCTIONS['my_func:v2:d']({'foo':'bar'}) == {'foo':'bar'}

    def test_generator(self):
        task = floto.specs.task.ActivityTask(domain='d', name='n', version='v')
        @floto.generator(domain='d', name='my_generator', version='v1')
        def my_generator(context):
            return [task]
        assert floto.ACTIVITY_FUNCTIONS['my_generator:v1:d']('context') == [task]

    def test_generator_raises(self):
        @floto.generator(domain='d', name='my_generator', version='v2')
        def my_generator(context):
            return 'result' 
        with pytest.raises(ValueError):
            floto.ACTIVITY_FUNCTIONS['my_generator:v2:d']('context')

    def test_compress_generator_result(self, init_response):
        result = {'foo':'bar'}
        floto.decorators.COMPRESS_GENERATOR_RESULT = True
        z = floto.decorators.compress_generator_result(result)
        floto.decorators.COMPRESS_GENERATOR_RESULT = False

        task = floto.specs.task.ActivityTask(domain='d', name='activity1', version='v1')
        b = floto.decider.DecisionBuilder(activity_tasks=[task], default_activity_task_list='floto_activities')
        b.history = floto.History('d', 'tl', init_response) 

        uncompressed = b._decompress_result(z)
        assert result == json.loads(uncompressed)

    def test_compress_generator_result_wo_compression(self):
        result = {'foo':'bar'}
        z = floto.decorators.compress_generator_result(result)
        assert z == result
