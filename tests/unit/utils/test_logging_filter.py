import pytest
import floto.utils.logging_filter
import os
import socket
import logging

@pytest.fixture
def log_record():
    kwargs = {'name':'MyLogger',
              'level':logging.DEBUG,
              'pathname':'path_to_file.py',
              'lineno':5,
              'msg':'Debug message',
              'args':{},
              'exc_info':None}
    return logging.LogRecord(**kwargs)

class TestFilterAddContext:
    def test_init(self):
        context = floto.utils.logging_filter.FilterAddContext()
        assert context.pid == os.getpid()
        assert context.fqdn == socket.getfqdn()

    def test_filter_adds_context(self, log_record):
        context = floto.utils.logging_filter.FilterAddContext()
        context.pid = 1234
        context.fqdn = 'hostname.com'

        assert context.filter(log_record)
        assert log_record.pid == '1234'
        assert log_record.fqdn == 'hostname.com'

class TestFilterAddIdentity:
    def test_init(self):
        context = floto.utils.logging_filter.FilterAddIdentity(identity='worker1')
        assert context.identity == 'worker1'

    def test_filter_adds_context(self, log_record):
        filter_ = floto.utils.logging_filter.FilterAddIdentity(identity='worker1')
        filter_.identity = 'decider'

        assert filter_.filter(log_record)
        assert log_record.identity == 'decider'

