import pytest
from unittest.mock import Mock

import floto.daemon
from collections import namedtuple
import random
import os.path
import argparse
import tempfile

class TestDaemon(floto.daemon.UnixDaemon):
    def __init__(self, command='start', pid_file='file.pid'):
        super().__init__()
        self._args = self.arg_parser.parse_args([command, '--pid_file', pid_file])

    # Do not fork for testing
    def daemonize(self):
        pass

    def run(self):
        pass

class TestUnixDaemon:
    def test_args(self):
        d = TestDaemon()
        assert d.arg_parser.description == d.description
        assert set(vars(d.args).keys()) == set(['command', 'pid_file'])

    def test_default_pid_file_name(self):
        d = TestDaemon() 
        d.args.pid_file = None
        assert d.pid_file == '/tmp/unix_daemon.pid'

    def test_get_pid_from_pid_file(self):
        with tempfile.NamedTemporaryFile(mode='w') as pid_file:
            pid_file.write('1234')
            pid_file.seek(0)
            d = TestDaemon('start', pid_file=pid_file.name)
            assert d.pid == 1234

    def test_pid_raises(self):
        with pytest.raises(FileNotFoundError) as e:
            d = TestDaemon('start', pid_file='pid_file_does_not_exist.pid')
            d.pid

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

    def test_start_daemonizes(self):
        d = TestDaemon(command='start')
        d._pid_file_exists = Mock(return_value=False)
        d._start_daemon = Mock()
        d.action()
        d._start_daemon.assert_called_once_with()

    def test_start_pid_file_exists_process_running_raises(self):
        d = TestDaemon(command='start')
        d._pid_file_exists = Mock(return_value=True)
        d._is_running = Mock(return_value=True)
        d._pid = '1234'
        with pytest.raises(RuntimeError):
            d.action()

    def test_start_pid_file_exists_process_not_running_raises(self):
        d = TestDaemon(command='start')
        d._pid_file_exists = Mock(return_value=True)
        d._is_running = Mock(return_value=False)
        d._pid = '1234'
        with pytest.raises(RuntimeError):
            d.action()

    def test_stop_with_existing_pid_file(self):
        pid_file = '/tmp/{}.pid'.format(str(random.randint(0, 100000)))
        open(pid_file, 'w').write('123456789')

        d = TestDaemon(command='stop', pid_file=pid_file)
        d._kill_process = Mock()
        d.action()
        d._kill_process.assert_called_once_with()
        assert not d._pid_file_exists()

    def test_stop_without_pid_file_does_not_raise(self):
        d = TestDaemon(command='stop')
        d.stop()

    def test_restart(self):
        d = TestDaemon()
        d.stop = Mock()
        d.start = Mock()
        d.restart()
        d.stop.assert_called_once_with()
        d.start.assert_called_once_with()

    def test_status_no_pid_file(self, capsys):
        d = TestDaemon()
        d._pid_file_exists = Mock(return_value=False)
        d.status()
        out, err = capsys.readouterr()
        assert out.strip() == 'DAEMON NOT RUNNING -- No pid_file: file.pid'

    def test_status_pid_file_and_not_running(self, capsys):
        d = TestDaemon()
        d._pid_file_exists = Mock(return_value=True)
        d._is_running = Mock(return_value=False)
        d.status()
        out, err = capsys.readouterr()
        assert out.strip() == 'DAEMON NOT RUNNiNG -- pid_file exists: file.pid'

    def test_status_pid_file_and_running(self, capsys):
        d = TestDaemon()
        d._pid_file_exists = Mock(return_value=True)
        d._is_running = Mock(return_value=True)
        d.pid = '1234'
        d.status()
        out, err = capsys.readouterr()
        assert out.strip() == 'DAEMON RUNNING -- pid: 1234'

    def test_action_calls(self, mocker):
        d = TestDaemon(command='start')
        d.start = Mock()
        d.action()
        d.start.assert_called_once_with()

    def test_action_raises_with_unknow_command(self, mocker):
        d = TestDaemon(command='do_something')
        with pytest.raises(AttributeError):
            d.action()

    def test_write_pid_file(self, mocker):
        mocker.patch('os.getpid', return_value=1234)
        with tempfile.NamedTemporaryFile(mode='w') as pid_file:
            d = TestDaemon(pid_file=pid_file.name)
            d._write_pid_file()
            assert d._pid_file_exists()
            pid = open(pid_file.name).read().strip()
            assert pid == '1234'
        
