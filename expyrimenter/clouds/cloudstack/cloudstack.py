from .api import API
from .statemonitor import StateMonitorProcess
from expyrimenter.core import Executor, SSH, Function
from time import sleep
import threading
import logging


class VMNotFound(Exception):
    pass


class CloudStack:
    _id_cache = None

    def __init__(s, executor=None, api=None):
        if executor is None:
            executor = Executor()
        if api is None:
            api = API()

        s.executor = executor
        s._api = api
        s._logger = logging.getLogger('cloudstack')

        s._sm_lock = threading.Lock()
        s._sm_tasks = 0

    def get_states(s):
        vms = s._list_vms()
        return {vm['name']: vm['state'] for vm in vms}

    # All not-found VM names are logged as error
    def get_id(s, name):
        cache = CloudStack._id_cache
        if cache is None or name not in cache:
            s.load_id_cache()

        if name not in CloudStack._id_cache:
            msg = 'VM "%s" not found.' % name
            s._logger.error(msg)
            raise VMNotFound(msg)

        return CloudStack._id_cache[name]

    # throws VMNotFound
    def get_state(s, name):
        vm_id = s.get_id(name)
        vms = s._list_vms(id=vm_id)
        return vms[0]['state']

    def start(s, *names):
        names = s._ensure_lists(names)
        for name in names:
            s._logger.info('Starting %s' % name)
            try:
                vm_id = s.get_id(name)
                s._submit_sm_task(start_vm, 'start VM ' + name, name, vm_id)
            except VMNotFound:
                pass  # do not quit the loop

    def stop(s, *names):
        names = s._ensure_lists(names)
        for name in names:
            try:
                vm_id = s.get_id(name)
                f = Function(stop_vm, name, vm_id)
                f.title = 'stop VM ' + name
                s.executor.run(f)
            except VMNotFound:
                pass  # do not quit the loop

    def get_deploy_params(s, name):
        params = {}
        vm_id = s.get_id(name)
        vms = s._list_vms(id=vm_id)
        vm = vms[0]

        deploy_keys = ['serviceofferingid', 'templateid', 'zoneid']
        for key in deploy_keys:
            params[key] = vm[key]
        return params

    def deploy(s, params, **kwargs):
        if kwargs:
            params.update(kwargs)
        name = params['name']
        s._submit_sm_task(deploy_vm, 'deploy ' + name, params)

    def deploy_like(s, existent, new, **kwargs):
        params = s.get_deploy_params(existent)
        params['name'] = new
        s.deploy(params, **kwargs)

    def load_id_cache(s):
        vms = s._list_vms()
        CloudStack._id_cache = {vm['name']: vm['id'] for vm in vms}
        return CloudStack._id_cache

    def _list_vms(s, **kwargs):
        try:
            vms = s._api.listVirtualMachines(**kwargs)['virtualmachine']
        except:
            s._logger.error('Error getting VM list.')
            vms = {}
        return vms

    def _submit_sm_task(s, fn, title, *args, **kwargs):
        with s._sm_lock:
            s._sm_tasks += 1
            StateMonitorProcess.start()

        states = StateMonitorProcess.get_states()
        # TODO why not append()?
        args += (states,)
        f = Function(fn, *args, **kwargs)
        f.title = title
        future = s.executor.run(f)
        future.add_done_callback(s._sm_task_done)

        return future

    def _sm_task_done(s, future):
        with s._sm_lock:
            s._sm_tasks -= 1
            if s._sm_tasks == 0:
                StateMonitorProcess.stop()

    def _ensure_lists(s, args):
        lists = []
        for arg in args:
            if isinstance(arg, list):
                lists += arg
            else:
                lists.append(arg)
        return lists


def stop_vm(name, vm_id):
    api = API()
    api.stopVirtualMachine(id=vm_id)
    msg = 'sent stop request for {}.'.format(name)
    logging.getLogger('cloudstack').debug(msg)


def start_vm(name, vm_id, states):
    api = API()
    api.startVirtualMachine(id=vm_id)
    wait_ssh(name, states)
    msg = name + ' is up.'
    logging.getLogger('cloudstack').info(msg)


def deploy_vm(params, states):
    api = API()
    api.deployVirtualMachine(**params)
    log = logging.getLogger('cloudstack')
    if 'startvm' in params and params['startvm'] is False:
        msg = params['name'] + '{} deployed.'
        log.debug(msg)
    else:
        wait_ssh(params['name'], states)
        msg = params['name'] + ' ready for SSH.'
        log.info(msg)


def wait_ssh(name, states):
    wait_state(name, 'Running', states)
    SSH.await_availability(name)


def wait_state(name, state, states, interval=5):
    while True:
        if state == states.get(name):
            break
        sleep(interval)
