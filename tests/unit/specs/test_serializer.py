import pytest
import floto.specs.serializer

class TestSerializer:
    def test_copy_dict(self):
        d = {'foo':'bar'}
        new_dict = floto.specs.serializer.copy_dict(d)
        assert d == new_dict
        assert d is not new_dict

    @pytest.mark.parametrize('dict_,expected,filter_',[
        ({'k1':'v'}, {}, ['k1']),
        ({}, {}, ['k1']),
        ({'k1':'v', 'k2':'v'}, {}, ['k1','k2']),
        ({'k':'v'}, {'k':'v'}, ['k2'])])
    def test_copy_dict_filters_keys(self, dict_, expected, filter_):
        new_dict = floto.specs.serializer.copy_dict(dict_, filter_)
        assert new_dict == expected


