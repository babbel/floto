# -*- coding: utf-8 -*-

import os
import logging
import sys
import time

import os.path
import uuid
import tempfile
import psutil
from signal import SIGTERM


logging.basicConfig(filename='my_daemon.log',level=logging.DEBUG)
logger = logging.getLogger(__name__)


##################
# Generic Daemon #
##################


class Daemon:
    """This needs to be inherited and run method overwritten to create a deamon.
    It forks the current process to get a deamon. 
    See 'Python Cookbook', Martelli et al. O'Reilly, 2nd edition, page: 388
    and https://github.com/ImmobilienScout24/succubus/blob/master/src/main/python/succubus/daemonize.py """
    

    def __init__(self, pid_file=None):
        
        if pid_file:
            self.pid_file = pid_file
        else:
            self.pid_file = '/tmp/tmp_pid_file.pid'      
        if len(sys.argv) != 2:
            raise Exception('Something is wrong, 2 parameters expected: <daemon.py> <start|stop|status> ')
        self.command = sys.argv[1]


    def daemonize(self, stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
        """ Creates a daemon."""
        logger.info('Daemonizing ...')
        try: 
            pid = os.fork()
            if pid > 0:
                sys.exit(0)    
            else:
                logger.error('oserror first parent fork, pid <= 0')  
        except OSError as err: 
            logger.error('oserror first parent fork')
            sys.exit(1) 

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
                sys.exit(0) 
                
        except OSError as err: 
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
            logger.error('IOError writing pid to pid file')          
        logger.info('... finishing daemonize method' )


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
                raise Exception('Something went wrong, try turn it on and off again' )

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
                sys.stdout.write('path to pid file: ' +  self.pid_file + '\n')
            else:
                sys.stdout.write('Something wrong, process ' + str(pid) +' not alive anymore. \n')
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


import floto.decider
import floto.specs
import floto

class Decider(Daemon):
    domain = 'floto_test'
    simple_task = floto.specs.task.ActivityTask(domain=domain, name='activity1', version='v5')
    decider_spec = floto.specs.DeciderSpec(domain=domain,
                                       task_list='simple_decider',
                                       default_activity_task_list='hello_world_atl',
                                       terminate_decider_after_completion=True,
                                       activity_tasks=[simple_task])

    decider = floto.decider.Decider(decider_spec=decider_spec)


    def run(self):
        decider.run()
        sys.stdout.write('current decider pid: ' + os.getpid())
        print('current decider pid: ' + os.getpid())

       
class Worker(Daemon):
    @floto.activity(domain=domain, name='activity1', version='v5')
    def simple_activity():
        print('\nSimpleWorker: I\'m working!')
        for i in range(3):
            print('.')
            time.sleep(0.8)

        # Terminate the worker after first execution:
        print('I\'m done.')


    def run(self):
        worker = floto.ActivityWorker(domain=domain, task_list='hello_world_atl')
        worker.run()



def main():
    d = Decider('tmp/decider_stuff.pid')
    sys.exit(d.action())


if __name__ == '__main__':
    main()