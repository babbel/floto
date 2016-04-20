# -*- coding: utf-8 -*-
import logging
import os
import sys
import time
from signal import SIGTERM

import psutil


logging.basicConfig(filename='daemon/my_daemon.log', level=logging.DEBUG)
logger = logging.getLogger(__name__)


##################
# Generic Daemon #
##################


class UnixDaemon:
    """This needs to be inherited and run method overwritten to create a deamon.
    It forks the current process to get a deamon.
    See 'Python Cookbook', Martelli et al. O'Reilly, 2nd edition, page: 388
    and https://github.com/ImmobilienScout24/succubus/blob/master/src/main/
    python/succubus/daemonize.py """

    def __init__(self, pid_file=None):
        if pid_file:
            self.pid_file = pid_file
        else:
            self.pid_file = '/tmp/tmp_pid_file.pid'
        if len(sys.argv) != 2:
            raise Exception('2 parameters expected: <daemon.py> <start|stop|status>')
        self.command = sys.argv[1]

    def daemonize(self, stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
        """ Creates a daemon."""
        # TODO: stdout and stderr to log files
        logger.info('Daemonizing ...')
        try:
            pid = os.fork()
            if pid > 0:
                sys.exit(0)
        except OSError as err:
            sys.stderr.write('First fork: OS error({0}): {1}'.format(err.errno, err.strerror))
            logger.error('First parent fork failed.')
            sys.exit(1)

        # decouple from parent environment
        os.setsid()
        os.umask(0)
        os.chdir("/")
        try:
            # Second fork
            pid = os.fork()
            if pid > 0:
                print('daemon: ', pid)
                sys.exit(0)
        except OSError as err:
            sys.stderr.write('Second fork: OS error({0}): {1}'.format(err.errno, err.strerror))
            logger.error('oserror second parent fork')
            sys.exit(1)

        # redirect standard file descriptors
        sys.stdout.flush()
        sys.stderr.flush()
        si = open(stdin, 'r')
        so = open(stdout, 'w')
        se = open(stderr, 'a+')
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())

        # write current daemon pid to file
        try:
            pid = str(os.getpid())
            with open(self.pid_file, 'w') as f:
                f.write(str(pid))
            f.close()
        except IOError as err:
            sys.stderr.write('IOError writing pid to pid file ({0}): {1}'.format(err.errno, err.strerror))
            logger.error('IOError writing pid to pid file')
        logger.info('... finishing daemonize method')

    def run(self):
        """to be overwritten"""
        raise NotImplementedError

    def stop(self):
        """Stop the daemon."""
        pid = self.get_pid_from_file()
        try:
            os.remove(self.pid_file)
            if pid:
               os.kill(pid, SIGTERM)
            else:
                sys.stdout.write('pid not found at: ' + self.pid_file + '. Is daemon running? \n')
        except:
            logger.error('Problem killing process')

    def get_pid_from_file(self):
        """Returns pid of current daemon."""
        if os.path.isfile(self.pid_file):
            try:
                pid = int(open(self.pid_file).read().strip())
                return pid
            except:
                logger.error('Problems reading in pid from pid file')
        else:
            return None

    def start(self):
        """Start the daemon."""
        if os.path.isfile(self.pid_file):
            pid = self.get_pid_from_file()
            if psutil.pid_exists(pid):
                sys.stdout.write('pid already exists')
                raise Exception('Seems daemon is already running')
            else:
                raise Exception('Something went wrong, try turn it on and off again')

        else:
            self.daemonize()
            self.run()

    def status(self):
        """Status of current daemon process. """
        # TODO: check if pid is in process list
        pid = self.get_pid_from_file()
        if pid:
            if psutil.pid_exists(pid):
                sys.stdout.write('daemon running with pid: ' + str(pid) + '\n')
                sys.stdout.write('path to pid file: ' + self.pid_file + '\n')
            else:
                sys.stdout.write('Something wrong, process ' + str(pid) + ' not alive anymore. \n')
        else:
            sys.stdout.write('No daemon running \n')

    def action(self):
        if self.command == 'start':
            self.start()
        if self.command == 'stop':
            self.stop()
        if self.command == 'status':
            self.status()


#######################
# Specify Deamon Task #
#######################

class MyDaemon(UnixDaemon):
    logger.info('here')
    print('active')

    def run(self):
        count = 0
        while True:
            # some stuff to be done by the daemon
            print('overwrite')
            count += 1
            time.sleep(1)
            logger.info(str(count) + '. pid: ' + str(os.getpid()))


def main():
    d = MyDaemon()
    sys.exit(d.action())


if __name__ == '__main__':
    main()
