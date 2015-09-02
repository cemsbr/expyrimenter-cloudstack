from .cloudstack import CloudStack
from expyrimenter.core import ExpyLogger


class Pool:
    RUNNING = 'Running'
    STOPPED = 'Stopped'

    def __init__(self, hostnames=None):
        self.hostnames = [] if hostnames is None else hostnames
        self._logger = ExpyLogger.getLogger('pool')
        self._cs = CloudStack()
        self._states = None  # hostname: state dict
        self._last_started = []

    @property
    def last_started(self):
        return self._last_started

    def update(self):
        self._states = None

    def get(self, amount):
        """Return VMs ready for SSH (blocking)."""
        running = self.running_vms
        to_start = amount - len(running)
        if to_start > 0:
            self._start_vms(to_start)
            running = self.running_vms
            assert len(running) == amount
        return running

    def stop(self):
        self._cs.stop(self.running_vms)
        self.update()

    def wait(self):
        self._cs.executor.wait()

    @property
    def states(self):
        if self._states is None:
            states = self._cs.get_states()
            self._states = {h: states[h] for h in self.hostnames}
        return self._states

    @property
    def running_vms(self):
        return self._vms_by_state(Pool.RUNNING)

    @property
    def stopped_vms(self):
        return self._vms_by_state(Pool.STOPPED)

    def _vms_by_state(self, state):
        """Select the VMs by state.

        :param state: RUNNING, STOPPED
        :rtype: list of hostnames
        """
        return [h for h in self.hostnames if self.states[h] == state]

    def _start_vms(self, amount):
        """Start stopped VMs.

        :param int n: Positive integer
        :returns: n started VM hostnames
        :rtype: list of strings
        """
        start_us = self.stopped_vms[:amount]
        self._cs.start(start_us)
        self._last_started = start_us
        self._logger.info('starting %d VMs', amount)
        self._cs.executor.wait()
        self.update()
