import pytest
import json
from floto.api import WorkflowType
import floto.api
import unittest.mock

class TestWorkflowType:
    def test_init_with_parameters(self):
        w = WorkflowType(domain='d', name='n', version='v1', description='desc')
        assert w.domain == 'd'
        assert w.name == 'n'
        assert w.version == 'v1'
        assert w.description == 'desc'
        assert w.default_child_policy == 'TERMINATE'
