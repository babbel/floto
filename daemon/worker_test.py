import logging

#logging.basicConfig(filename='my_worker.log',level=logging.DEBUG)
logger = logging.getLogger(__name__)

import floto
from daemon import Daemon
import sys
import time

class Worker(Daemon):
    domain = 'floto_test'

    @floto.activity(domain=domain, name='activity1', version='v5')
    def simple_activity():
        print('\nSimpleWorker: I\'m working!')
        for i in range(3):
            print('.')
            time.sleep(0.8)

        # Terminate the worker after first execution:
        print('I\'m done.')


    def run(self):
        logging.basicConfig(filename='my_worker.log',level=logging.DEBUG)
        worker = floto.ActivityWorker(domain=self.domain, task_list='hello_world_atl')
        worker.run()


def main():
    #Worker().run()
    d = Worker('/tmp/worker_stuff.pid')
    sys.exit(d.action())


if __name__ == '__main__':
    main()
