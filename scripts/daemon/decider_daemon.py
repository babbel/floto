import logging

import floto.decider
from floto.daemon import UnixDaemon

class DeciderDaemon(UnixDaemon):
    def __init__(self):
        super().__init__()
        self.daemon_description = 'Daemon to run floto deciders'

        if self.args.log_config:
            self.configure_logger(self.args.log_config)

    def add_parse_arguments(self, parser):
        super().add_parse_arguments(parser)
        parser.add_argument('decider_spec', help='Path to decider specification file')
        parser.add_argument('--log_config', help='Path to the logging config file')

    def get_decider_from_spec_file(self, path):
        # TODO handle s3 paths
        with open(path, 'r') as f:
            decider_spec = f.read()
        return floto.decider.Decider(decider_spec=decider_spec)

    def run(self):
        decider = self.get_decider_from_spec_file(self.args.decider_spec)
        decider.run()

    def configure_logger(self, path):
        if path.endswith('.yml'):
            if os.path.exists(path):
                with open(path, 'rt') as f:
                    config = yaml.safe_load(f.read())
            logging.config.dictConfig(config)


def main():
    d = DeciderDaemon()
    d.action()

if __name__ == '__main__':
    main()
