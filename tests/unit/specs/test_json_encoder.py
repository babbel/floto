import pytest
import floto.specs
import floto.specs.retry_strategy
import json

@pytest.fixture(scope='module')
def activity_task_json():
    t = {'type': 'floto.specs.ActivityTask', 
         'activity_id': 'activity_id', 
         'name': 'n'}
    return json.dumps(t)

class TestJSONEncoder(object):
    def test_dictionary(self):
        obj = {'foo':'bar'}
        assert json.dumps(obj, cls=floto.specs.JSONEncoder) == json.dumps(obj)

    @pytest.mark.parametrize('task_args, expected',
            [({'name':'n', 'version':'v'}, {'name':'n', 'version':'v'}),
             ({'input':{'foo':'bar'}}, {'input':{'foo':'bar'}})])
    def test_activity_task(self, task_args, expected):
        task = floto.specs.ActivityTask(**task_args)
        result = json.loads(json.dumps(task, cls=floto.specs.JSONEncoder))
        assert all(item in result.items() for item in expected.items()) 

    def test_activity_tasks_requires(self):
        t1 = floto.specs.ActivityTask(name='t1')
        t2 = floto.specs.ActivityTask(name='t2', requires=[t1])
        result = json.loads(json.dumps(t2, cls=floto.specs.JSONEncoder))
        assert result['requires'][0]['name'] == 't1' 

    def test_default_encoder(self):
        obj = {'foo':'bar'}
        j = json.loads(json.dumps(obj, cls=floto.specs.JSONEncoder))
        assert j['foo'] == 'bar'

    def test_decider_spec(self):
        tasks = [floto.specs.ActivityTask(name='n')]
        spec = floto.specs.DeciderSpec(activity_tasks=tasks)

        json_spec = json.loads(json.dumps(spec, cls=floto.specs.JSONEncoder))
        assert json_spec['activity_tasks'][0]['name'] == 'n' 

    def test_retry_strategy_dump(self):
        s = floto.specs.retry_strategy.InstantRetry(retries=3)
        j = json.dumps(s, cls=floto.specs.JSONEncoder)
        json_spec = json.loads(j)
        assert json_spec['type'] == 'floto.specs.retry_strategy.InstantRetry'
        assert json_spec['retries'] == 3

    def test_retry_strategy_loads(self):
        j = {"type": "floto.specs.retry_strategy.InstantRetry", 
             "retries": 3}
        s = json.loads(json.dumps(j), object_hook=floto.specs.JSONEncoder.object_hook) 
        assert s.is_task_resubmitted(failures=3)

    def test_timer_dump(self):
        t = floto.specs.Timer(id_='my_timer', requires=['t'])
        j = json.dumps(t, cls=floto.specs.JSONEncoder)
        json_spec = json.loads(j)
        assert json_spec['type'] == 'floto.specs.Timer'
        assert json_spec['id_'] == 'my_timer'

    def test_child_workflow_dump(self):
        cw = floto.specs.ChildWorkflow(workflow_id='wid')
        j = json.dumps(cw, cls=floto.specs.JSONEncoder)
        json_spec = json.loads(j)
        assert json_spec['type'] == 'floto.specs.ChildWorkflow'
        assert json_spec['id_'] == 'wid'

    def test_child_workflow_loads(self):
        j = {'type':'floto.specs.ChildWorkflow', 'id_':'wid'}
        cw = json.loads(json.dumps(j), object_hook=floto.specs.JSONEncoder.object_hook) 
        assert isinstance(cw, floto.specs.ChildWorkflow)
        assert cw.id_ == 'wid'

    def test_generator_dumps(self):
        g = floto.specs.Generator(activity_id='generator_id')
        j = json.dumps(g, cls=floto.specs.JSONEncoder)
        json_spec = json.loads(j)
        assert json_spec['type'] == 'floto.specs.Generator'
        assert json_spec['id_'] == 'generator_id'

    def test_generator_loads(self):
        j = {'type':'floto.specs.Generator', 'id_':'generator_id'}
        g = json.loads(json.dumps(j), object_hook=floto.specs.JSONEncoder.object_hook) 
        assert isinstance(g, floto.specs.Generator)
        assert g.id_ == 'generator_id'

    def test_workflow_spec(self):
        tasks = [floto.specs.ActivityTask(name='n')]
        spec = floto.specs.DeciderSpec(activity_tasks=tasks)
        workflow_spec = {'decider_spec':spec}
        json_spec = json.loads(json.dumps(workflow_spec, cls=floto.specs.JSONEncoder))
        assert json_spec['decider_spec']['type'] == 'floto.specs.DeciderSpec'

    def test_type(self):
        task = floto.specs.ActivityTask(name='n')
        result = json.loads(json.dumps(task, cls=floto.specs.JSONEncoder))
        assert result['type'] == 'floto.specs.ActivityTask'

    def test_nested_type(self):
        task = floto.specs.ActivityTask(name='n')
        decider_spec = floto.specs.DeciderSpec(activity_tasks=[task])
        json_spec = json.dumps(decider_spec, cls=floto.specs.JSONEncoder, indent=2)
        result = json.loads(json_spec)
        assert result['activity_tasks'][0]['type'] == 'floto.specs.ActivityTask'

    def test_object_creation(self, activity_task_json):
        t = json.loads(activity_task_json, object_hook=floto.specs.JSONEncoder.object_hook)
        assert isinstance(t, floto.specs.ActivityTask)
        assert t.name == 'n'


    @pytest.mark.parametrize('old,update,new',
            [({},                     {'foo':'bar'},           {'foo':'bar'}),
             ({},                     {'foo':{'foo2':'bar'}},  {'foo':{'foo2':'bar'}}),
             ({'foo':'bar'},          {},                      {'foo':'bar'}),
             ({'foo':{'foo2':'bar'}}, {'foo':{'foo2':'bar2'}}, {'foo':{'foo2':'bar2'}}),
             ({'f':{'f2':'b'}, 'f3':'b2'}, {'f':{'f2':'b2'}},  {'f':{'f2':'b2'},'f3':'b2'}) ])
    def test_update_dict(self, old, update, new):
        assert floto.specs.JSONEncoder.update_dict(old, update) == new

    def test_update_namespace(self):
        j = type("TestClass", (object,), {})()
        floto.specs.JSONEncoder.update_namespace(j, {'foo':'bar', 'foo2':{'foo3':'bar2'}})
        assert j.foo == 'bar'
        assert j.foo2 == {'foo3':'bar2'}
 
    def test_load_simple_string(self):
        s = 'hello world'
        j = floto.specs.JSONEncoder.load_string(s)
        assert j == s

    def test_load_json_string(self):
        s = json.dumps({'hello':'world'})
        j = floto.specs.JSONEncoder.load_string(s)
        assert j == {'hello':'world'} 

    @pytest.mark.parametrize('obj', [({'foo':'bar'}), ('s')])
    def test_dump_object(self, obj):
        s = floto.specs.JSONEncoder.dump_object(obj)
        j = floto.specs.JSONEncoder.load_string(s)
        assert j == obj

    def test_dump_string(self):
        j = floto.specs.JSONEncoder.dump_object('s')
        assert j == 's'

    def test_dump_task(self):
        obj = floto.specs.ActivityTask()
        s =  floto.specs.JSONEncoder.dump_object(obj)
        t = json.loads(s, object_hook=floto.specs.JSONEncoder.object_hook)
        assert isinstance(t, floto.specs.ActivityTask)


