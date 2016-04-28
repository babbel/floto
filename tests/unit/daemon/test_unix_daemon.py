import logging
import pytest
import floto.daemon
from collections import namedtuple
import random
import time
import os.path

class TestDaemon(floto.daemon.UnixDaemon):
    def __init__(self, command, pid_file=None):
        super().__init__()
        args = {'command':command,
                'pid_file':pid_file}
        self.args = namedtuple('args', args.keys())(**args)
        self.logger = logging.getLogger('TestLogger')
        self.run_ongoing_task = False

    def run(self):
        self.logger.debug('run()')
        while self.run_ongoing_task:
            time.sleep(1)

class TestUnixDaemon:
    def test_start_is_called(self, mocker):
        mocker.patch('floto.daemon.UnixDaemon.start')
        d = floto.daemon.UnixDaemon()
        d.args = namedtuple('args', ['command'])(command='start')
        d.action()
        d.start.assert_called_once_with()

    def test_run_cleans_up(self):
        pid_file = '/tmp/{}.pid'.format(str(random.randint(0, 100000)))
        test_daemon = TestDaemon(command='start', pid_file=pid_file)
        test_daemon.run_ongoing_task = False
        test_daemon.action()
        assert not os.path.isfile(test_daemon.pid_file)

