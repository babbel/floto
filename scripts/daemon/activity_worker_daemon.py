import logging
import importlib.util

import floto
from floto.daemon import UnixDaemon

class ActivityWorkerDaemon(UnixDaemon):
    def add_parse_arguments(self, parser):
        super().add_parse_arguments(parser)
        parser.add_argument('domain', help='SWF domain of the worker')
        parser.add_argument('task_list', help='Worker task list')
        parser.add_argument('activities', help='Path to activity function definition')
        parser.add_argument('--identity', help='Identity of the worker')
        parser.add_argument('--heartbeat', help='Task heatbeat in seconds')
        parser.add_argument('--log_config', help='Path to the logging config file')

    def register_activities(self):
        # TODO read from s3
        spec = importlib.util.spec_from_file_location('activities', self.args.activities)
        activities = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(activities)

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
        self.register_activities()
        worker = self.get_worker()
        worker.run()

    def configure_logger(self, path):
        if path.endswith('.yml'):
            if os.path.exists(path):
                with open(path, 'rt') as f:
                    config = yaml.safe_load(f.read())
            logging.config.dictConfig(config)

def main():
    ActivityWorkerDaemon().action()

if __name__ == '__main__':
    main()
