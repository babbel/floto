import pytest
import floto.decisions

@pytest.fixture
def decision():
    return floto.decisions.Decision()

class TestDecision():
    def test_path_in_dictionary(self, decision):
        dictionary = {'key':'value'}
        assert decision.path_in_dictionary(dictionary, 'key')
        assert not decision.path_in_dictionary(dictionary, 'key2')

    def test_path_in_dictionary_several_keys(self, decision):
        dictionary = {'key':{'key2':'val'}}
        assert decision.path_in_dictionary(dictionary, 'key.key2')

    def test_path_in_dictionary_several_keys(self, decision):
        dictionary = {'key':{'key2':'val'}}
        assert decision.path_in_dictionary(dictionary, 'key.key2')


