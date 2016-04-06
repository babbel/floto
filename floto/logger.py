import logging
import logging.config

class Logger:
    def __init__(self, name, identity=None):
        self.logger = logging.getLogger(name)
        self.identity = identity

    @staticmethod
    def getLogger(name, identity=None):
        return Logger(name, identity)

    def debug(self, msg):
        self.logger.debug(self.add_identity(msg))

    def warning(self, msg):
        self.logger.warning(self.add_identity(msg))

    def error(self, msg):
        self.logger.error(self.add_identity(msg))

    def add_identity(self, msg):
        new_msg = ''
        if self.identity:
            new_msg = '{}:{}'.format(self.identity, msg)
        else:
            new_msg = msg
        return new_msg


