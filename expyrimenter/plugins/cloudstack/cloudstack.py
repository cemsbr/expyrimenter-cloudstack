from .api import API
from .statemonitor import StateMonitorProcess
from time import sleep
import threading
from expyrimenter.core import SSH, Executor, Function, ExpyLogger


class VMNotFound(Exception):
    pass


class CloudStack:
    """Currently, only API calls are blocking. To block everything, call wait()
    in the executor attribute.
    """
    _id_cache = None

    def __init__(self, executor=None, api=None, logger_name=None):
        if executor is None:
            executor = Executor()
        if api is None:
            api = API()
        if logger_name is None:
            logger_name = 'cloudstack'

        self.executor = executor
        self._api = api
        self._logger_name = logger_name
        self._logger = ExpyLogger.getLogger(name=logger_name)

        # sm vars are related to State Monitor
        self._sm_lock = threading.Lock()
        self._sm_tasks = 0

    def get_states(self, **kwargs):
        vms = self._list_vms(**kwargs)
        return {vm['name']: vm['state'] for vm in vms}

    # throws VMNotFound
    def get_state(self, name):
        vm_id = self.get_id(name)
        vms = self._list_vms(id=vm_id)
        return vms[0]['state']

    # All not-found VM names are logged as error
    def get_id(self, name):
        cache = CloudStack._id_cache
        if cache is None or name not in cache:
            self.load_id_cache()

        if name not in CloudStack._id_cache:
            msg = 'VM "{}" not found.'.format(name)
            self._logger.error(msg)
            raise VMNotFound(msg)

        return CloudStack._id_cache[name]

    def start(self, *names):
        names = ensure_list(names)
        for vm in names:
            title = 'start VM ' + vm
            try:
                vm_id = self.get_id(vm)
                self._submit_sm_task(self.start_vm, title, vm_id, vm)
            except VMNotFound:
                pass  # Already logged in get_id. Do not quit the loop.

    def stop(self, *names):
        names = ensure_list(names)
        for vm in names:
            title = 'stop VM ' + vm
            try:
                vm_id = self.get_id(vm)
                self._submit_task(self.stop_vm, title, vm_id)
            except VMNotFound:
                pass  # Already logged in get_id. Stop next VMs.

    def deploy_like(self, existent, new, **kwargs):
        params = self.get_deploy_params(existent)
        params['name'] = new
        self.deploy(params, **kwargs)

    def get_deploy_params(self, name):
        params = {}
        vm_id = self.get_id(name)
        vms = self._list_vms(id=vm_id)
        vm = vms[0]

        deploy_keys = ['serviceofferingid', 'templateid', 'zoneid']
        for key in deploy_keys:
            params[key] = vm[key]
        return params

    def deploy(self, params, **kwargs):
        if kwargs:
            params.update(kwargs)
        vm = params['name']
        self._submit_sm_task(self.deploy_vm, 'deploy VM ' + vm, params)

    def load_id_cache(self):
        vms = self._list_vms()
        CloudStack._id_cache = {vm['name']: vm['id'] for vm in vms}

    def _list_vms(self, **kwargs):
        try:
            vms = self._api.listVirtualMachines(**kwargs)['virtualmachine']
        except Exception as e:
            self._logger.failure('list VMs', e)
            vms = {}
        return vms

    def _submit_sm_task(self, fn, title, *args, **kwargs):
        with self._sm_lock:
            self._sm_tasks += 1
            if self._sm_tasks == 1:
                StateMonitorProcess.start(interval=10)

        future = self._submit_task(fn, title, *args, **kwargs)
        future.add_done_callback(self._sm_task_done)

        return future

    def _submit_task(self, fn, title, *args, **kwargs):
        f = Function(fn, title=title, logger_name=self._logger_name)
        f.set_args(*args, **kwargs)
        return self.executor.run(f)

    # pylint: disable=unused-argument
    def _sm_task_done(self, future):
        with self._sm_lock:
            self._sm_tasks -= 1
            if self._sm_tasks == 0:
                StateMonitorProcess.stop()

    def start_vm(self, vm_id, vm):
        self._api.startVirtualMachine(id=vm_id)
        self.wait_ssh(vm)

    def stop_vm(self, vm_id):
        self._api.stopVirtualMachine(id=vm_id)

    def deploy_vm(self, params):
        self._api.deployVirtualMachine(**params)
        vm = params['name']
        self.wait_ssh(vm)

    def wait_ssh(self, vm, interval=10):
        self.wait_state(vm, 'Running', interval)
        SSH.await_availability(vm, interval)

    def wait_state(self, vm, state, interval):
        states = StateMonitorProcess.get_states()
        while True:
            if state == states.get(vm):
                break
            sleep(interval)


def ensure_list(args):
    l = []
    for arg in args:
        if isinstance(arg, list):
            l += arg
        else:
            l.append(arg)
    return l
