# -*- coding: utf-8 -*-

import os
import logging
import sys
import time
import atexit
import os.path
import uuid
import tempfile
from signal import SIGTERM


logging.basicConfig(filename='my_daemon.log',level=logging.DEBUG)
logger = logging.getLogger(__name__)


##################
# Generic Daemon #
##################


class Daemon:
    """This needs to be inherited and the run method overwritten to create a deamon.
    It forks the current process to get a deamon. 
    See 'Python Cookbook', Martelli et al. O'Reilly, 2nd edition, page: 388
    and https://github.com/ImmobilienScout24/succubus/blob/master/src/main/python/succubus/daemonize.py """
    

    def __init__(self, pid_file=None):
        
        if pid_file:
            self.pid_file = pid_file
        else:
            unique_filepart = str(uuid.uuid4())
            # [TODO] tempfile if no fileparameter is set
            self.pid_file = '/tmp/tmp_pid_file' + unique_filenpart + '.pid'
        
        if len(sys.argv) != 2:
            raise Exception('Something is wrong, 2 parameters expected: <daemon.py> <command> ')
        self.command = sys.argv[1]



    def daemonize(self, stdin='/dev/null', stdout='/Users/franziskaadler/Code/Deamon/daemon/my_daemon_trail.txt', stderr='/dev/null'):
        """ Creates a daemon."""
        logger.info('... daemonizing')
        #[TODO] set stdout, stderr to log file, if somehow possible
        sys.stdout.write('beginning..' + str(os.getpid()) + ' \n')

        try: 
            parent_pid = os.getpid() # just for logging, to be deleted later
            pid = os.fork()

            if pid > 0: # good pid
                logger.info('exit first parent pid ' + str(parent_pid) + ', forked child: ' + str(pid))
                sys.stdout.write('beginning..1 ' + str(os.getpid()) + ' \n')
                sys.stdout.write('before first exit' + str(os.getpid()))
                sys.exit(0) 
                
        except OSError as err: 
            logger.error('oserror first parent fork')
            sys.exit(1) # exit with error

        logger.info('exit first parent pid ' + str(parent_pid) + ', forked child: ' + str(pid))

        # decouple from parent environment

        os.setsid() 
        os.umask(0) 
        os.chdir("/") 



        try: 
            # Second fork - prevent you from accidentally reacquiring a controlling terminal
            parent_pid = os.getpid()
            pid = os.fork()
            if pid > 0:
                print('daemon: ', pid)
                logger.info('exit second parent pid ' + str(parent_pid) + ', forked child/ daemon: ' + str(pid))
                sys.exit(0) 
                
        except OSError as err: 
            logger.error('oserror second parent fork')
            sys.exit(1) # exit with error

        logger.info('exit second parent pid ' + str(parent_pid) + ', forked child: ' + str(pid))

        # redirect standard file descriptors
        sys.stdout.flush()
        sys.stderr.flush()
        si = open(stdin, 'r')
        so = open(stdout, 'w')
        se = open(stderr, 'a+')
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())



        sys.stdout.write('yeah')


        # write current daemon pid to file
        try:
            pid = str(os.getpid())
            with open(self.pid_file, 'w') as f:
                f.write(str(pid))
            f.close()
        except IOError as err:
            logger.error('IOError writing pid to pid file')
            
        logger.info('finishing daemonize method' )


    def run(self):
        """to be overwritten"""
        raise NotImplementedError


    def stop(self): 
        """Stop the daemon."""
        
        pid = self.get_pid_from_file()
        if pid:
           try:                
               os.kill(pid, SIGTERM)
               os.remove(self.pid_file)
           except:
               logger.error('Problem killing process')       
        else:
            sys.stdout.write('pid file not found')


    def get_pid_from_file(self):

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
        print(self.pid_file)
        self.daemonize() # TODO catch errors and edge cases...
        self.run()


    def status(self):

        # TODO: check if pid is in process list
        pid = self.get_pid_from_file()
        if pid:
            sys.stdout.write('daemon running with pid: ' + str(pid) + '\n')
            sys.stdout.write('path to pid: ' +  self.pid_file + '\n')
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


class MyDaemon(Daemon):
    def run(self):
        count = 0
        while True:
            # some stuff to be done by the daemon
            count += 1
            time.sleep(1)
            logger.info(str(count) + '. pid: ' + str(os.getpid()))


def main():
    d = MyDaemon('/Users/franziskaadler/Code/Deamon/daemon/pid_file.pid')
    sys.exit(d.action())


if __name__ == '__main__':
    main()