import pytest
from floto.api import SwfType

class TestSwfType:
    def test_init_with_parameters_raises(self):
        """Make sure that the base type doesn't set defaults"""
        with pytest.raises(TypeError):
            t = SwfType(domain='d', name='my_name', version='1')
