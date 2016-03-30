import pytest
import floto.specs

class TestJSONSerializable:
    def test_type(self):
        j = floto.specs.JSONSerializable().serializable()
        assert j['type'] == 'floto.specs.JSONSerializable'

    def test_deserialized(self):
        serializable = floto.specs.JSONSerializable().serializable()
        j = floto.specs.JSONSerializable.deserialized(**serializable)
        assert isinstance(j, floto.specs.JSONSerializable)

    def test_deserialized_raises(self):
        serializable = floto.specs.JSONSerializable().serializable()
        serializable['field1'] = 'value1'
        with pytest.raises(TypeError):
            j = floto.specs.JSONSerializable.deserialized(**serializable)


