from .api import API
from multiprocessing import Manager, Process
from time import sleep
import signal
from expyrimenter.core import ExpyLogger


class StateMonitor:
    _stop = False

    def __init__(self, states_proxy):
        self._states_proxy = states_proxy
        self._local_states = {}
        self._api = API()
        self._logger = ExpyLogger.getLogger('cloudstack.statemonitor')
        self.title = '{} {}'.format(type(self).__name__, id(self))
        self._logger.start(self.title)

    def monitor_states(self, interval=5):
        while not StateMonitor._stop:
            self._monitor_states_once()
            sleep(interval)
        self._logger.end(self.title)

    @classmethod
    def stop(cls):
        cls._stop = True

    def _monitor_states_once(self):
        vms = self._api.listVirtualMachines()['virtualmachine']
        for vm in vms:
            self._update_state(vm['name'], vm['state'])

    def _update_state(self, k, v):
        if v != self._local_states.get(k):
            self._local_states[k] = v
            self._states_proxy[k] = v

    @staticmethod
    def handler(signum, frame):
        StateMonitor.stop()

    @staticmethod
    def state_monitor_proc(states):
        sm = StateMonitor(states)
        sm.monitor_states()


signal.signal(signal.SIGTERM, StateMonitor.handler)


# Ensures only one state monitor is running at most
class StateMonitorProcess:
    _mgr = _states = _process = None

    @classmethod
    def start(cls):
        """Not thread-safe. Should be called from the same process/thread."""
        if cls._process is None:
            cls._mgr = Manager()
            cls._states = cls._mgr.dict()
            cls._process = Process(target=StateMonitor.state_monitor_proc,
                                   args=(cls._states, ))
            cls._process.start()

    @classmethod
    def stop(cls):
        if cls._process is not None:
            cls._process.terminate()
            cls._process.join()
            cls._states.clear()
            cls._mgr.shutdown()
            cls._mgr = cls._states = cls._process = None

    @classmethod
    def get_states(cls):
        return cls._states
