import pytest
import floto
from unittest.mock import Mock
import multiprocessing as mp
import time

class HeartbeatSenderMock(floto.HeartbeatSender):
    def __init__(self):
        super().__init__()
        self.number_heartbeats = mp.Value('i', 0)

    def _send_heartbeat(self, timeout, task_token):
        while self.is_send_heartbeat.value:
            try:
                self.swf.record_activity_task_heartbeat(task_token=task_token, details=None) 
            except Exception:
                pass
            time.sleep(timeout)
            self.number_heartbeats.value += 1 

@pytest.fixture
def heartbeat_sender(mocker):
    swf = type("ClientMock", (object,), {'record_activity_task_heartbeat':Mock()})
    sender = floto.HeartbeatSender()
    sender.swf = swf
    return sender
    
@pytest.fixture
def heartbeat_sender_mock(mocker):
    swf = type("ClientMock", (object,), {'record_activity_task_heartbeat':Mock()})
    sender = HeartbeatSenderMock()
    sender.swf = swf
    return sender

class TestHeartbeatSender(object):
    def test_init(self):
        hbs = floto.HeartbeatSender()
        assert hbs

    def test_send_heartbeats(self, mocker, heartbeat_sender_mock):
        heartbeat_sender_mock.send_heartbeats(0.1, 'tt')
        time.sleep(0.35)
        heartbeat_sender_mock.stop_heartbeats()
        assert heartbeat_sender_mock.number_heartbeats.value == 3
        
    def test__send_heartbeat(self, heartbeat_sender):
        stopper = mp.Process(target=self.stop_heartbeat, args=(0.05, heartbeat_sender))
        stopper.start()
        heartbeat_sender._send_heartbeat(timeout=0.1, task_token='tt')
        stopper.join()
        args = {'details':None,
                'task_token':'tt'}
        heartbeat_sender.swf.record_activity_task_heartbeat.assert_called_once_with(**args)

    def test_send_heartbeat_does_not_raise(self, heartbeat_sender_mock):
        raises = Mock(side_effect=Exception)
        heartbeat_sender_mock.swf.record_activity_task_heartbeat = raises
        heartbeat_sender_mock.send_heartbeats(0.1, 'tt')
        time.sleep(0.35)
        heartbeat_sender_mock.stop_heartbeats()
        assert heartbeat_sender_mock.number_heartbeats.value == 3


    def stop_heartbeat(self, timeout, sender):
        print('trying to stop heartbeats')
        time.sleep(timeout)
        sender.stop_heartbeats()





