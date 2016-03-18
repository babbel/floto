import pytest
import floto.specs

class TestGenerator:
    def test_init(self, mocker):
        mocker.patch('floto.specs.ActivityTask.__init__')
        args = {'name':'n',
                'version':'v',
                'activity_id':'aid',
                'requires':['a'],
                'input':{'foo':'bar'},
                'retry_strategy':'rs'}
        g = floto.specs.Generator(**args)
        floto.specs.ActivityTask.__init__.assert_called_once_with(**args)

