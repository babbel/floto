import ctypes
import multiprocessing as mp
import time

import floto.api


class HeartbeatSender:
    def __init__(self):
        self.swf = floto.api.Swf()
        self.is_send_heartbeat = mp.Value(ctypes.c_bool, True)
        self.process = None

    def send_heartbeats(self, timeout, task_token):
        self.is_send_heartbeat.value = True
        self.process = mp.Process(target=self._send_heartbeat, args=(timeout, task_token))
        self.process.start()

    def stop_heartbeats(self):
        self.is_send_heartbeat.value = False

    def _send_heartbeat(self, timeout, task_token):
        while self.is_send_heartbeat.value:
            try:
                response = self.swf.record_activity_task_heartbeat(task_token=task_token, details=None)
            except Exception:
                pass
            time.sleep(timeout)
