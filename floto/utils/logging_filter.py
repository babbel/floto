import logging
import socket
import os

class FilterAddContext(logging.Filter):
    def __init__(self):
        self.pid = os.getpid()
        self.fqdn = socket.getfqdn()

    def filter(self, log_record):
        log_record.pid = str(self.pid)
        log_record.fqdn = str(self.fqdn)
        return True


class FilterAddIdentity(logging.Filter):
    def __init__(self, identity):
        self.identity = identity

    def filter(self, log_record):
        log_record.identity = str(self.identity)
        return True



