import pytest
import floto.specs.task
import floto.specs.retry_strategy


@pytest.fixture
def generator():
    rs = floto.specs.retry_strategy.InstantRetry(retries=2)
    args = {'name':'n',
            'version':'v',
            'domain': 'd',
            'id_':'g_id',
            'input':{'foo':'bar'},
            'retry_strategy':rs}
    g = floto.specs.task.Generator(**args)
    return g

class TestGenerator:
    def test_init(self, mocker):
        mocker.patch('floto.specs.task.ActivityTask.__init__')
        args = {'name':'n',
                'version':'v',
                'domain': 'd',
                'id_':'aid',
                'requires':['a'],
                'input':{'foo':'bar'},
                'retry_strategy':'rs'}
        g = floto.specs.task.Generator(**args)
        floto.specs.task.ActivityTask.__init__.assert_called_once_with(**args)

    def test_serializable(self, generator):
        s = generator.serializable()
        assert s['type'] == 'floto.specs.task.Generator'

    def test_deserialized(self):
        d = {'version': 'v', 
             'retry_strategy': {'retries': 2, 'type': 'floto.specs.retry_strategy.InstantRetry'}, 
             'type': 'floto.specs.task.Generator', 
             'domain': 'd', 
             'name': 'n', 
             'id_': 'g_id', 
             'input': {'foo': 'bar'}}
        g = floto.specs.task.Generator.deserialized(**d)
        assert isinstance(g, floto.specs.task.Generator)
        assert g.id_ == 'g_id'
        assert isinstance(g.retry_strategy, floto.specs.retry_strategy.InstantRetry)

