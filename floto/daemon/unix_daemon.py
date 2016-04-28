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
    It forks the current process to get a deamon.
    See 'Python Cookbook', Martelli et al. O'Reilly, 2nd edition, page: 388
    and https://github.com/ImmobilienScout24/succubus/blob/master/src/main/
    python/succubus/daemonize.py """

    def __init__(self):
        self._logger = None
        self._args = None
        self._arg_parser = None
        self._pid_file = None
        self.daemon_description = 'Base unix daemon'
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
            self._arg_parser = argparse.ArgumentParser(description=self.daemon_description)
            self.add_parse_arguments(self._arg_parser)
        return self._arg_parser

    @arg_parser.setter
    def arg_parser(self, parser):
        self._arg_parser = parser

    @property
    def pid_file(self):
        if self.args.pid_file:
            self._pid_file = self.args.pid_file
        else:
            self._pid_file = '/tmp/unix_daemon.pid'
        return self._pid_file

    @pid_file.setter
    def pid_file(self, pid_file):
        self._pid_file = pid_file

    def add_parse_arguments(self, parser):
        """Definition of the command line arguments of the base daemon.
        To add arguments in a child class overwrite this method and call:
        super().add_parse_arguments(parser)
        before.
        """
        parser.add_argument('command', help='The daemon command: start|status|stop|restart')
        parser.add_argument('--pid_file', help='The pid_file of the daemon')
    
    def fork(self):
        try:
            pid = os.fork()
            # If in parent process exit
            if pid > 0:
                os._exit(0)
        except OSError as err:
            self.logger.error('Fork failed.')
            os._exit(1)

    def daemonize(self, stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
        """ Creates a daemon."""
        # TODO: stdout and stderr to log files
        self.fork()

        # decouple from parent environment with new session id
        os.setsid()
        os.umask(0)
        os.chdir("/")

        # Second fork to make process 'non session-leader' 
        self.fork()

        # # TODO
        # redirect standard file descriptors
        # sys.stdout.flush()
        # sys.stderr.flush()
        # si = open(stdin, 'r')
        # so = open(stdout, 'w')
        # se = open(stderr, 'a+')
        # os.dup2(si.fileno(), sys.stdin.fileno())
        # os.dup2(so.fileno(), sys.stdout.fileno())
        # os.dup2(se.fileno(), sys.stderr.fileno())

        self.write_pid_file()

    def write_pid_file(self):
        # write current daemon pid to file
        try:
            pid = str(os.getpid())
            self.logger.debug('Writing pid to file: {}'.format(pid))
            with open(self.pid_file, 'w') as f:
                f.write(str(pid))
            f.close()
        except IOError as err:
            msg = 'IOError writing pid to pid file ({0}): {1}'.format(err.errno, err.strerror)
            self.logger.error(msg)

    def run(self):
        """to be overwritten"""
        raise NotImplementedError

    def stop(self):
        """Stop the daemon."""
        pid = self.get_pid_from_file()
        self._delete_pid_file()
        if pid:
            self._kill_process(pid)

    def get_pid_from_file(self):
        """Returns pid of current daemon."""
        pid = None
        if os.path.isfile(self.pid_file):
            try:
                pid = int(open(self.pid_file).read().strip())
            except:
                self.logger.error('Problems reading in pid from pid file')
        else:
            self.logger.error('pid_file does not exist: {}'.format(self.pid_file))
        return pid

    def start(self):
        """Start the daemon."""
        if os.path.isfile(self.pid_file):
            pid = self.get_pid_from_file()
            if self._pid_exists(pid):
                msg = 'Daemon already running: {}'.format(pid)
                self.logger.error(msg)
                raise Exception(msg)
            else:
                msg = 'pid_file exists: {}, but process is not running.'.format(self.pid_file)
                self.logger.error(msg)
                raise Exception(msg)
        else:
            self.daemonize()
            self.run()

            # Finished run(), remove pid_file 
            self._delete_pid_file() 

    def status(self):
        """Status of current daemon process. """
        pid = self.get_pid_from_file()
        if pid:
            if self._pid_exists(pid):
                sys.stdout.write('daemon running with pid: ' + str(pid) + '\n')
                sys.stdout.write('path to pid file: ' + self.pid_file + '\n')
            else:
                sys.stdout.write('Something wrong, process ' + str(pid) + ' not alive anymore. \n')
        else:
            sys.stdout.write('No daemon running \n')

    def restart(self):
        self.stop()
        self.start()

    def action(self):
        try:
            getattr(self, self.args.command)()
        except AttributeError:
            msg = 'Command must be start|status|stop|restart. Was {}'.format(self.args.command)
            self.logger.error(msg)

    def _pid_exists(self, pid):
        """Check for the existence of a unix pid."""
        try:
            # Process is not killed, os.kill(pid, 0) does nothing but raise if process does not
            # exist.
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        else:
            return True

    def _kill_process(self, pid):
        try:
            for _ in range(100):
                os.kill(pid, SIGTERM)
                time.sleep(0.1)
        except ProcessLookupError:
            return
        except OSError as err:
            self.logger.error(msg)

    def _delete_pid_file(self):
        try:
            os.remove(self.pid_file)
        except FileNotFoundError:
            self.logger.error('pid_file does not exist: {}'.format(self.pid_file))

