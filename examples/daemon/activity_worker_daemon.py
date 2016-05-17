import logging
import importlib.util

import floto
from floto.daemon import UnixDaemon
import os
import yaml

@floto.activity(domain='floto_test', name='simple_activity', version='v1')
def simple_activity():
    print('\nSimpleWorker: I\'m working!')
    for i in range(3):
        print('.')
        time.sleep(0.8)

    # Terminate the worker after first execution:
    print('I\'m done.')

class ActivityWorkerDaemon(UnixDaemon):
    def __init__(self):
        super().__init__()
        self.daemon_description = 'Daemon to run floto worker to copy S3 folders.'
        self.configure_logger()

    def add_parse_arguments(self, parser):
        super().add_parse_arguments(parser)
        parser.add_argument('domain', help='SWF domain of the worker')
        parser.add_argument('task_list', help='Worker task list')
        parser.add_argument('--identity', help='Identity of the worker')
        parser.add_argument('--heartbeat', help='Task heatbeat in seconds')
        parser.add_argument('--log_config', help='Path to the logging config file')

    def get_worker(self):
        args = {'domain':self.args.domain,
                'task_list':self.args.task_list}

        if self.args.identity:
            args['identity'] = self.args.identity

        if self.args.heartbeat:
            args['task_heartbeat_in_seconds'] = self.args.heartbeat

        return floto.ActivityWorker(**args)

    def run(self):
        self.logger.debug('starting worker')
        worker = self.get_worker()
        worker.run()

    def configure_logger(self):
        path = self.args.log_config if self.args.log_config else ''
        if path.endswith('.yml'):
            if os.path.exists(path):
                with open(path, 'rt') as f:
                    config = yaml.safe_load(f.read())
            else:
                raise ValueError('Path does not exist: {}'.format(path))
            logging.config.dictConfig(config)

def main():
    ActivityWorkerDaemon().action()

if __name__ == '__main__':
    main()
