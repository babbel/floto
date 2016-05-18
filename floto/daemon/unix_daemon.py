import logging
import logging.config
import os
import sys
import time
import argparse

from signal import SIGTERM

class UnixDaemon:
    """A generic unix daemon. This class needs to be inherited and the run method overwritten to 
    create a specific deamon.
    
    Example (my_daemon.py)
    Run with: python my_daemon.py start|stop|restart|status
    ----------------------

    class MyDaemon(UnixDaemon):
        def __init__(self):
        super().__init__()
        # Overwrite description, appears in e.g. python my_daemon.py -h
        self.daemon_description = 'My daemon'

        # Add you own script arguments.
        def add_parse_arguments(self, parsre):
            super().add_parse_arguments(parser)

            # Example:
            # Make log config available in self.args.log_config
            parser.add_argument('--log_config', help='Path to logging config file')

        def run(self):
            # Your code
            pass

    if __name__ = '__main__':
        MyDaemon().action()
    """

    def __init__(self):
        self._args = None
        self._arg_parser = None
        self._pid_file = None
        self._pid = None
        self.description = 'Base unix daemon'
        self.logger = logging.getLogger(__name__)

    @property
    def args(self):
        if not self._args:
            self._args = self.arg_parser.parse_args()
        return self._args

    @args.setter
    def args(self, args):
        self._args = args

    @property
    def arg_parser(self):
        if not self._arg_parser:
            self.arg_parser = argparse.ArgumentParser(description=self.description)
            self.add_parse_arguments(self._arg_parser)
        return self._arg_parser

    @arg_parser.setter
    def arg_parser(self, parser):
        self._arg_parser = parser

    @property
    def pid_file(self):
        if self.args.pid_file:
            self.pid_file = self.args.pid_file
        else:
            self._pid_file = '/tmp/unix_daemon.pid'
        return self._pid_file

    @pid_file.setter
    def pid_file(self, pid_file):
        self._pid_file = pid_file

    @property
    def pid(self):
        if not self._pid:
            try:
                self.pid = int(open(self.pid_file).read().strip())
            except FileNotFoundError as e:
                self.logger.error('Can not read pid from file {}'.format(self.pid_file))
                raise e
        return self._pid

    @pid.setter
    def pid(self, pid):
        self._pid = pid

    def add_parse_arguments(self, parser):
        """Definition of the command line arguments of the base daemon.
        To add arguments in a child class overwrite this method and call:
        super().add_parse_arguments(parser)
        before.
        """
        parser.add_argument('command', help='The daemon command: start|status|stop|restart')
        parser.add_argument('--pid_file', help='The pid_file of the daemon')
    
    def create_child_process(self):
        try:
            pid = os.fork()
            # In parent process id of child > 0 is returned: exit parent
            if pid > 0:
                os._exit(0)
        except OSError as err:
            self.logger.error('Fork failed.')
            os._exit(1)

    def daemonize(self):
        """ Creates a daemon."""
        self.create_child_process()

        # Decouple from parent environment with new session id
        os.setsid()
        os.umask(0)
        os.chdir("/")

        # Second child process to make process 'non session-leader' 
        self.create_child_process()

    def run(self):
        """to be overwritten"""
        raise NotImplementedError

    def start(self):
        """Start the daemon."""
        if self._pid_file_exists():
            if self._is_running():
                msg = 'Daemon already running: {}'.format(self.pid)
            else:
                msg = 'pid_file exists: {}, but process is not running.'.format(self.pid_file)
            self.logger.error(msg)
            raise RuntimeError(msg)
        else:
            self._start_daemon()

    def stop(self):
        """Stop the daemon."""
        if self._pid_file_exists():
            self._kill_process()
            os.remove(self.pid_file)
        else:
            msg = 'Trying to stop daemon with non existing pid_file: {}'.format(self.pid_file)
            self.logger.warning(msg)

    def restart(self):
        self.stop()
        self.start()

    def status(self):
        """Status of current daemon process. """
        if self._pid_file_exists() and self._is_running():
            print('DAEMON RUNNING -- pid: {}'.format(self.pid)) 

        if self._pid_file_exists() and not self._is_running():
            print('DAEMON NOT RUNNiNG -- pid_file exists: {}'.format(self.pid_file))

        if not self._pid_file_exists():
            print('DAEMON NOT RUNNING -- No pid_file: {}'.format(self.pid_file)) 


    def action(self):
        try:
            getattr(self, self.args.command)()
        except AttributeError:
            msg = 'Command must be start|status|stop|restart. Was {}'.format(self.args.command)
            self.logger.error(msg)
            raise(AttributeError(msg))

    def _start_daemon(self):
        self.daemonize()
        self._write_pid_file()
        self.run()

        # Finished run(), remove pid_file 
        os.remove(self.pid_file)

    def _write_pid_file(self):
        """Write current daemon pid to file."""
        try:
            pid = str(os.getpid())
            self.logger.debug('Writing pid to file: {}'.format(pid))
            with open(self.pid_file, 'w') as f:
                f.write(str(pid))
            f.close()
        except IOError as err:
            msg = 'IOError writing pid to pid file ({0}): {1}'.format(err.errno, err.strerror)
            self.logger.error(msg)

    def _pid_file_exists(self):
        return True if os.path.isfile(self.pid_file) else False

    def _is_running(self):
        """Check if process with self.pid is running."""
        try:
            # Process is not killed, os.kill(pid, 0) does nothing but raise if process does not
            # exist.
            os.kill(self.pid, 0)
        except ProcessLookupError:
            return False
        else:
            return True

    def _kill_process(self):
        try:
            for _ in range(100):
                os.kill(self.pid, SIGTERM)
                time.sleep(0.1)
        except ProcessLookupError:
            return
        except OSError as err:
            self.logger.error(msg)

