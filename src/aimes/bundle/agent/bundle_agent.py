# -*- coding: utf-8 -*-

import Queue
import threading
import logging
import socket
import datetime
import time
import os
import xml.etree.ElementTree as ET
import re
import json

import paramiko

from aimes.bundle                 import BundleException


class RemoteBundleAgent(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self._sampling_interval = 60
        self.queue = Queue.Queue()

    def set_verbosity(self, verbose):
       self.debug_level = verbose
       if verbose == 0:
           self.logger.setLevel(logging.ERROR)
       elif verbose == 1:
           self.logger.setLevel(logging.INFO)
       elif verbose > 1:
           self.logger.setLevel(logging.DEBUG)

    def setup_logger(self, verbose):
        self.logger = logging.getLogger(uid)
        if not self.logger.handlers:
            fmt = '%(asctime)-15s {} %(name)-8s %(levelname)-8s %(funcName)s:%(lineno)-4s %(message)s'.format(self._uid)
            formatter = logging.Formatter(fmt)
            ch = logging.StreamHandler()
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)
        self.set_verbosity(verbose)
        self.debug = self.logger.debug
        self.info = self.logger.info
        self.warn = self.logger.warn
        self.error = self.logger.error
        self.critical = self.logger.critical
        self.exception = self.logger.exception

    def setup_ssh_connection(self, credential):
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh.connect( hostname=self._login_server,
                          port=credential.get('port', 22),
                          username=credential.get('username'),
                          password=credential.get('password'),
                          key_filename=credential.get('key_filename') )
        self.cmd_prefix = ''

    def run_cmd(self, cmd, timeout=30):
        try:
            chan = self.ssh.get_transport().open_session()
            chan.settimeout(timeout)
            #chan.set_combine_stderr(True)
            chan.exec_command(self.cmd_prefix + cmd)

            stdout = ''
            while True:
                recv_buf = chan.recv(65536)
                if len(recv_buf) == 0:
                    break
                else:
                    stdout += recv_buf

            stderr = ''
            while True:
                recv_buf = chan.recv_stderr(65536)
                if len(recv_buf) == 0:
                    break
                else:
                    stderr += recv_buf

            exit_status = chan.recv_exit_status()
        except socket.timeout:
            print "Timeout executing {} after {} seconds".format(cmd, timeout)
            chan.close()
            raise
        else:
            return exit_status, stdout, stderr

    def start_cmd_loop(self):
        self.setDaemon(True)
        self.start()

    def start_timer(self):
        self.queue.put("update_config")
        self.queue.put("update_workload")
        self._timer = threading.Timer(self._sampling_interval, self.start_timer)
        self._timer.setDaemon(True)
        self._timer.start()

    def run(self):
        while True:
            cmd = self.queue.get()
            if cmd is "update_config":
                self._update_config()
                self.queue.task_done()
            elif cmd is "update_workload":
                self._update_workload()
                self.queue.task_done()
            elif cmd is "close":
                if self._timer:
                    self._timer.cancel()
                self.queue.task_done()
                break
            else:
                print "Unkown command {}".format(cmd)
                self.queue.task_done()

    def close(self):
        if self.queue:
            self.queue.put("close")
            self.queue.join()
            self.queue = None
        if self.ssh:
            self.ssh.close()
            self.ssh = None

class MoabAgent(RemoteBundleAgent, threading.Thread): pass

backdoor_cluster_num_nodes = {
    'stampede.tacc.xsede.org'  : 6400, # https://www.tacc.utexas.edu/user-services/user-guides/stampede-user-guide
    'trestles.sdsc.xsede.org'  :  324, # http://www.sdsc.edu/us/resources/trestles/trestles_jobs.html
    'gordon.sdsc.xsede.org'    : 1024, # http://www.sdsc.edu/supercomputing/gordon/using_gordon/allocations.html
    'hopper.sdsc.xsede.org'    : 1024, # https://www.nersc.gov/users/computations-systems/hopper/
    'blacklight.psc.xsede.org' :  256, # http://www.psc.edu/index.php/computing-resources/blacklight#hardware
    'hopper.nersc.gov'         : 6384, # https://www.nersc.gov/users/computational-systems/hopper/configuration/compute-nodes/
}

backdoor_trestles_properties_queues = {
    'exclusive' : ['normal'],
    'exclusive,work' : ['normal'],
    'shared' : ['shared'],
    'shared,abaqus' : ['shared'],
}

backdoor_trestles_queue_limits = {
    'num_procs_limit' : 32 * 32,
    'max_walltime' : 48 * 3600,
}

backdoor_gordon_interested_queues = [
    'normal', 'vsmp',
]

backdoor_gordon_queue_num_procs_limits = {
    'normal' : 64 * 16,
    'vsmp' : 1 * 16,
}

backdoor_gordon_properties_queues = {
    'native,flash' : ['normal'],
    'native,noflash' : ['normal'],
    'native,bigflash' : ['normal'],
    'vsmp' : ['vsmp'],
}

backdoor_blacklight_interested_queues = [
    'batch', 'batch_r', 'batch_r1', 'batch_r2', 'batch_l1', 'batch_l2',
    # 'debug', 'debug_r', 'debug_r1',
]

backdoor_hopper_interested_queues = [
    'debug', 'regular', 'low', 'thruput', 'scavenger', 'premium',
]

backdoor_hopper_queue_jobs_limits = {
    'debug' : 2,
    'regular' : 32,
    'low' : 12,
    'thruput' : 500,
    'scavenger' : 2,
    'premium' : 1,
}

backdoor_hopper_queue_num_procs_limits = {
    'debug' : 512 * 24,
    'regular' : 4096 * 24,
    'low' : 683 * 24,
    'thruput' : 2 * 24,
    'scavenger' : 683 * 24,
    'premium' : 2048 * 24,
}

class PbsAgent(RemoteBundleAgent, threading.Thread):
    my_type = 'pbs'
    sampling_interval = 270 #completed job 5:00

    def __init__(self, credential, verbose=0, db_dir=None):
        self.hostname = credential['login_server']
        self.queue = None
        super(PbsAgent, self).setup_logger(self.hostname, verbose)
        super(PbsAgent, self).setup_ssh_connection(credential)

        # TODO move following outside __init__
        self.get_queue_config()
        self.get_workload()
        self.connect_to_db()
        return

        # start daemon thread, keep updating db
        threading.Thread.__init__(self, name=self.hostname + " bundle agent")
        self.queue = Queue.Queue()
        self.setDaemon(True)
        self.start()

        if False:
            #Set peoriodical sampling timer
            self._timer = threading.Timer(self.sampling_interval, self.sampling_timer)
            self._timer.setDaemon(True)
            self._timer.start()

    def __del__(self):
        # self.debug("disconnect from {}".format(self.hostname))
        self.close()

    def cluster_type(self):
        return self.my_type

    def get_configuration(self):
        return {
            'num_nodes' : self.num_nodes,
            'queue_info' : self.get_queue_config(),
        }

    def get_queue_config(self, flag=0):
        exit_status, stdout, stderr = self.run_cmd("qstat -Q -f")
        if exit_status != 0:
            err_msg = "'qstat -Q -f' return ({}): stdout({}), stderr({})".format(
                exit_status, stdout, stderr)
            self.error(err_msg)
            return None
        else:
            _queue_info = {}

            for line_str in stdout.splitlines():
                if line_str.find('Queue') == 0:
                    pos = line_str.find(':')
                    queue_name = line_str[pos+1:].strip()
                    _queue_info[queue_name] = {'queue_name' : queue_name}

                elif line_str.find('=') != -1:
                    queue_attr_k = line_str.lstrip()[:line_str.lstrip().find(' ')]
                    pos = line_str.find('=')
                    queue_attr_v = line_str[pos+1:].strip()

                    if queue_attr_k == 'acl_user_enable':
                        if (queue_attr_v == 'True'):
                            _queue_info[queue_name]['restricted_access'] = True

                    # elif queue_attr_k == 'resources_assigned.nodect':
                    #     _queue_info[queue_name]['num_nodes'] = int(queue_attr_v)

                    elif queue_attr_k == 'resources_max.walltime':
                        if queue_attr_v.count(':') == 2:
                            h, m, s = [int(i) for i in queue_attr_v.split(':')]
                            _queue_info[queue_name]['max_walltime'] = 3600*h+60*m+s
                        elif queue_attr_v.count(':') == 3:
                            d, h, m, s = [int(i) for i in queue_attr_v.split(':')]
                            _queue_info[queue_name]['max_walltime'] = 24*3600*d+3600*h+60*m+s

                    elif queue_attr_k == 'resources_max.ncpus':
                        _queue_info[queue_name]['num_procs_limit'] = int(queue_attr_v)

                    elif queue_attr_k == 'started' or queue_attr_k == 'enabled':
                        if queue_attr_v == 'True':
                            _queue_info[queue_name][queue_attr_k] = True
                        else:
                            _queue_info[queue_name][queue_attr_k] = False

                    elif queue_attr_k == 'state_count' and flag == 1:
                        pos = queue_attr_v.find('Queued:')
                        _queue_info[queue_name]['num_queueing_jobs'] = int(
                            queue_attr_v[pos + len('Queued:') : queue_attr_v.find(' ', pos + len('Queued:'))])
                        pos = queue_attr_v.find('Running:')
                        _queue_info[queue_name]['num_running_jobs'] = int(
                            queue_attr_v[pos + len('Running:') : queue_attr_v.find(' ', pos + len('Running:'))])

            # ignore common user un-access-able queues
            for k,v in _queue_info.items():
                if  ('gordon'     in self.hostname and v['queue_name'] not in backdoor_gordon_interested_queues)     or \
                    ('blacklight' in self.hostname and v['queue_name'] not in backdoor_blacklight_interested_queues) or \
                    ('hopper'     in self.hostname and v['queue_name'] not in backdoor_hopper_interested_queues)     or \
                    (('restricted_access' in v) and (v['restricted_access'] == True)) or \
                    (('enabled'           in v) and (v['enabled']           != True)) or \
                    (('started'           in v) and (v['started']           != True)):
                    del _queue_info[k]
                    continue

                if 'enabled' in v:
                    del _queue_info[k]['enabled']
                if 'started' in v:
                    del _queue_info[k]['started']

            if 'trestles' in self.hostname:
                for v in _queue_info.values():
                    v['max_walltime'] = backdoor_trestles_queue_limits['max_walltime']
                    v['num_procs_limit'] = backdoor_trestles_queue_limits['num_procs_limit']
            elif 'gordon' in self.hostname:
                for k, v in _queue_info.items():
                    v['num_procs_limit'] = backdoor_gordon_queue_num_procs_limits[k]
            elif 'hopper' in self.hostname:
                for k, v in _queue_info.items():
                    v['queued_jobs_limit'] = backdoor_hopper_queue_jobs_limits[k]
                    v['num_procs_limit'] = backdoor_hopper_queue_num_procs_limits[k]
            elif 'blacklight' in self.hostname:
                _max_walltime = _num_procs_limit = 0
                for k, v in _queue_info.items():
                    if v.has_key('max_walltime') and v['max_walltime'] > _max_walltime:
                        _max_walltime = v['max_walltime']
                    if v.has_key('num_procs_limit') and v['num_procs_limit'] > _num_procs_limit:
                        _num_procs_limit = v['num_procs_limit']
                _queue_info['batch']['max_walltime'] = _max_walltime
                _queue_info['batch']['num_procs_limit'] = _num_procs_limit

                for k,v in _queue_info.items():
                    if k != 'batch':
                        del _queue_info[k]
            # tmp
            # if flag == 0:
            #     print json.dumps(_queue_info, sort_keys=True, indent=4)
            self.queue_info = _queue_info
            return self.queue_info

    # TODO add more error handling, what if the queue is empty?
    def qstat_x(self):
        exit_status, stdout, stderr = self.run_cmd("qstat -x")
        if exit_status != 0:
            err_msg = "'qstat -a -x' return ({}): stdout({}), stderr({})".format(
                exit_status, stdout, stderr)
            self.error(err_msg)
            return None
        else:
            queue_nodes = {}
            root = ET.fromstring(stdout)
            for job in root.findall('Job'):
                _queue = job.find('queue').text
                if 'hopper' in self.hostname:
                    _interested_queues = backdoor_hopper_interested_queues
                    if 'reg' in _queue:
                        _queue = 'regular'
                elif 'blacklight' in self.hostname:
                    _interested_queues = backdoor_blacklight_interested_queues
                    if 'batch' in _queue:
                        _queue = 'batch'
                    # elif 'debug' in _queue:
                    #     _queue = 'debug'

                if _queue in _interested_queues:
                    if _queue not in queue_nodes:
                        queue_nodes[_queue] = {
                            'num_busy_procs' : 0,
                            'num_queueing_jobs' : 0,
                            'num_running_jobs' : 0,
                        }
                    _job_state = job.find('job_state').text
                    try:
                        if _job_state == 'Q':
                            queue_nodes[_queue]['num_queueing_jobs'] += 1
                        elif _job_state == 'R' or _job_state == 'E':
                            queue_nodes[_queue]['num_running_jobs'] += 1
                            if 'blacklight' in self.hostname:
                                queue_nodes[_queue]['num_busy_procs'] += int(job.find('Resource_List').find('ncpus').text)
                            elif 'hopper' in self.hostname:
                                queue_nodes[_queue]['num_busy_procs'] += int(job.find('Resource_List').find('mppwidth').text)
                    except Exception as e:
                        pass
            return queue_nodes

    def get_workload_hopper(self):
        self.get_queue_config(1)
        queue_nodes = self.qstat_x()
        if queue_nodes:
            workload = {}
            for _queue in backdoor_hopper_interested_queues:
                workload[_queue] = {
                    'alive_nodes' : backdoor_cluster_num_nodes['hopper.nersc.gov'],
                    'alive_procs' : backdoor_cluster_num_nodes['hopper.nersc.gov'] * 24,
                    'busy_nodes' : 0,
                    'busy_procs' : 0,
                    'free_nodes' : backdoor_cluster_num_nodes['hopper.nersc.gov'],
                    'free_procs' : backdoor_cluster_num_nodes['hopper.nersc.gov'] * 24,
                    'num_queueing_jobs' : 0,
                    'num_running_jobs' : 0,
                }
            for k, v in queue_nodes.items():
                workload[k]['busy_nodes'] += (v['num_busy_procs'] / 24)
                workload[k]['busy_procs'] += (v['num_busy_procs'])
                workload[k]['free_nodes'] -= (v['num_busy_procs'] / 24)
                workload[k]['free_procs'] -= (v['num_busy_procs'])
                workload[k]['num_queueing_jobs'] += (v['num_queueing_jobs'])
                workload[k]['num_running_jobs'] += (v['num_running_jobs'])

            # tmp
            # print json.dumps(workload, sort_keys=True, indent=4)
            self.workload = workload
            return workload
        else:
            return None

    def get_workload_blacklight(self):
        self.get_queue_config(1)
        queue_nodes = self.qstat_x()
        if queue_nodes:
            workload = {
                'batch' : {
                    'alive_nodes' : 256,
                    'alive_procs' : 256 * 16,
                    'busy_nodes' : 0,
                    'busy_procs' : 0,
                    'free_nodes' : 256,
                    'free_procs' : 256 * 16,
                    'num_queueing_jobs' : 0,
                    'num_running_jobs' : 0,
                }
                # 'debug' : {
                #     'alive_nodes' : 256,
                #     'alive_procs' : 256 * 16,
                #     'busy_nodes' : 0,
                #     'busy_procs' : 0,
                #     'free_nodes' : 256,
                #     'free_procs' : 256 * 16,
                #     'num_queueing_jobs' : 0,
                #     'num_running_jobs' : 0,
                # }
            }
            for k, v in queue_nodes.items():
                aggregate_queue = k
                if 'batch' in k:
                    aggregate_queue = 'batch'
                # elif 'debug' in k:
                #     aggregate_queue = 'debug'
                else:
                    continue

                workload[aggregate_queue]['busy_nodes'] += (v['num_busy_procs'] / 16)
                workload[aggregate_queue]['busy_procs'] += (v['num_busy_procs'])
                workload[aggregate_queue]['free_nodes'] -= (v['num_busy_procs'] / 16)
                workload[aggregate_queue]['free_procs'] -= (v['num_busy_procs'])
                workload[aggregate_queue]['num_queueing_jobs'] += (v['num_queueing_jobs'])
                workload[aggregate_queue]['num_running_jobs'] += (v['num_running_jobs'])

            # tmp
            # print json.dumps(workload, sort_keys=True, indent=4)
            self.workload = workload
            return workload
        else:
            return None

    # run pbsnodes -a to collect info of node status
    def pbsnodes(self):
        exit_status, stdout, stderr = self.run_cmd("pbsnodes -a -x")
        if exit_status != 0:
            err_msg = "'pbsnodes -a -x' return ({}): stdout({}), stderr({})".format(
                exit_status, stdout, stderr)
            self.error(err_msg)
            return None
        else:
            root = ET.fromstring(stdout)
            node_list={}
            for node in root.findall('Node'):
                #if state == 'free' or state == 'job-exclusive':
                try:
                    node_list[node.find('name').text] = [
                        node.find('state').text.strip(),
                        int(node.find('np').text.strip()),
                        node.find('properties').text.strip(),
                        node.find('jobs').text.strip() if node.find('jobs') != None else None
                        ]
                except Exception as e:
                    pass

            return node_list

    def get_workload(self):
        if 'blacklight' in self.hostname:
            return self.get_workload_blacklight()
        elif 'hopper' in self.hostname:
            return self.get_workload_hopper()

        self.get_queue_config(1)

        if 'trestles' in self.hostname:
            properties_queues = backdoor_trestles_properties_queues
        elif 'gordon' in self.hostname:
            properties_queues = backdoor_gordon_properties_queues

        nl = self.pbsnodes()
        if nl:
            workload = {}
            for k, v in self.queue_info.items():
                workload[k] = {
                    'alive_nodes' : 0,
                    'alive_procs' : 0,
                    'busy_nodes' : 0,
                    'busy_procs' : 0,
                    'free_nodes' : 0,
                    'free_procs' : 0,
                    'num_queueing_jobs' : v['num_queueing_jobs'],
                    'num_running_jobs' : v['num_running_jobs'],
                    }

            for k, v in nl.items():
                state, np, properties, jobs = v
                if properties is not None :
                    if properties in properties_queues:
                        for _q in properties_queues[properties]:
                            if state == 'free' and jobs == None:
                                workload[_q]['free_nodes'] += 1
                                workload[_q]['free_procs'] += np
                                workload[_q]['alive_nodes'] += 1
                                workload[_q]['alive_procs'] += np
                            elif state == 'job-exclusive' or \
                                (state == 'free' and jobs != None):
                                workload[_q]['busy_nodes'] += 1
                                workload[_q]['busy_procs'] += np
                                workload[_q]['alive_nodes'] += 1
                                workload[_q]['alive_procs'] += np
                    else:
                        self.error("unknown node properties: {}".format(properties))

            # tmp
          # print json.dumps(workload, sort_keys=True, indent=4)
            self.workload = workload
            return workload
        else:
            return None

    def sampling_timer(self):
        self.queue.put("showq")
        #TODO check thread healthy
        self._timer = threading.Timer(self.sampling_interval, self.sampling_timer)
        self._timer.setDaemon(True)
        self._timer.start()

    def run(self):
        self.throt1 = 0
        while True:
            cmd = self.queue.get()
            if cmd is "showq":
                #TODO handle failure case
                #self.debug("run 'showq -c' periodically")
                try:
                    self.exec_showq3()
                except Exception as e:
                    self.throt1+=1
                    if self.throt1 < 10:
                        self.exception('periodically collect complete task trace failed: {} {}'.format(e.__class__, e))
                self.queue.task_done()
                #TODO add lock
            elif cmd is "close":
                #self.debug('received "close" command')
                if self._timer:
                    self._timer.cancel()
                #self.debug('timer canceled')
                self.queue.task_done()
                #self.debug('task done')
                break
            else:
                self.error("Unknown command {0}".format(str(cmd)))
                self.queue.task_done()
                continue

    def get_default_queue(self):
        exit_status, stdout, stderr = self.run_cmd('qmgr -c "p s"')
        if exit_status == 0: # success
            for line_str in stdout.splitlines():
                if line_str.find('set server default_queue') != -1:
                    pos1 = line_str.find('=')
                    self.default_queue = line_str[pos1+1:].strip()
                    #self.debug("{} default queue: {}".format(
                    #    self.hostname, self.default_queue))
                    break
        else:
            self.error(stderr)
        return self.default_queue

    def connect_to_db(self):
        pass

    @property
    def num_nodes(self):
        return backdoor_cluster_num_nodes[self.hostname]

backdoor_stampede_interested_queues = [
    'normal', 'development', 'largemem', 'serial', 'large',
]

backdoor_stampede_queue_proc_per_node = {
    'normal' : 16,
    'development' : 16,
    'largemem' : 32,
    'serial' : 16,
    'large' : 16,
}

backdoor_stampede_queue_num_procs_limits = {
    'normal' : 256 * 16,
    'development' : 16 * 16,
    'largemem' : 4 * 32,
    'serial' : 1 * 16,
    'large' : 1024 * 16,
}

backdoor_stampede_queue_jobs_limits = {
    'normal' : 50,
    'development' : 1,
    'largemem' : 4,
    'serial' : 8,
    'large' : 50,
}

backdoor_stampede_properties_queues = {
    'native,flash' : ['normal'],
    'native,noflash' : ['normal'],
    'native,bigflash' : ['normal'],
    'vsmp' : ['vsmp'],
}

class SlurmAgent(RemoteBundleAgent, threading.Thread):
    my_type = 'slurm'

    def __init__(self, credential, verbose=0, db_dir=None):
        self.hostname = credential['login_server']
        self.queue = None
        super(SlurmAgent, self).setup_logger(self.hostname, verbose)
        super(SlurmAgent, self).setup_ssh_connection(credential)

        self.get_queue_config()
        self.get_workload()
        self.connect_to_db()
        return

        #Start sampling thread
        threading.Thread.__init__(self, name=self.hostname + " bundle agent")
        self.queue = Queue.Queue()
        self.setDaemon(True)
        self.start()

    def __del__(self):
        #self.debug("Delete slurm cluster on {0}".format(self.hostname))
        self.close()

    def cluster_type(self):
        return self.my_type

    def get_configuration(self):
        return {
            'num_nodes' : self.num_nodes,
            'queue_info' : self.get_queue_config(),
        }

    def get_login_server(self):
        exit_status, stdout, stderr = self.run_cmd('hostname')
        self.login_server = stdout.splitlines()[0].strip()

    def get_queue_config(self, flag=0):
        exit_status, stdout, stderr = self.run_cmd('sinfo -o "%20P %5a %.10l %20F"')
        if exit_status != 0:
            err_msg = "'sinfo' return ({}): stdout({}), stderr({})".format(
                exit_status, stdout, stderr)
            self.error(err_msg)
            return None
        else:
            _queue_info = {}
            for line_str in stdout.splitlines()[1:]:
                _partition, _avail, _timelimit, _nodes = line_str.split()
                _partition=_partition.rstrip('*')
                if flag == 0:
                    r = re.compile('[-:]+')
                    d, h, m, s = [int(i) for i in r.split(_timelimit)]
                    _timelimit = 24*3600*d+3600*h+60*m+s
                    if 'stampede' in self.hostname and _partition in backdoor_stampede_interested_queues:
                        _queue_info[_partition] = {'queue_name' : _partition, 'max_walltime' : _timelimit, \
                            'num_procs_limit' : backdoor_stampede_queue_num_procs_limits[_partition], \
                            'queued_jobs_limit' : backdoor_stampede_queue_jobs_limits[_partition]
                        }
                else:
                    if 'stampede' in self.hostname and _partition in backdoor_stampede_interested_queues:
                        _active_nodes, _idle_nodes, _other_nodes, _total_nodes = [int(n) for n in _nodes.split('/')]
                        _queue_info[_partition] = {'queue_name' : _partition, 'active_nodes': _active_nodes, \
                            'idle_nodes' : _idle_nodes, 'other_nodes' : _other_nodes, 'total_nodes' : _total_nodes }
            # tmp
          # if flag == 0:
          #     print json.dumps(_queue_info, sort_keys=True, indent=4)
            self.queue_info = _queue_info
            return _queue_info

    def get_workload(self):
        self.get_queue_config(1)
        exit_status, stdout, stderr = self.run_cmd('squeue -r -o "%.18i %.13P %.13T %.6D"')
        if exit_status != 0:
            err_msg = "'squeue' return ({}): stdout({}), stderr({})".format(
                exit_status, stdout, stderr)
            self.error(err_msg)
            return None
        else:
            workload = {}
            if 'stampede' in self.hostname:
                for k, v in self.queue_info.items():
                    workload[k] = {
                        'alive_nodes' : v['total_nodes'] - v['other_nodes'],
                        'alive_procs' : (v['total_nodes'] - v['other_nodes']) * backdoor_stampede_queue_proc_per_node[k],
                        'busy_nodes' : v['active_nodes'],
                        'busy_procs' : v['active_nodes'] * backdoor_stampede_queue_proc_per_node[k],
                        'free_nodes' : v['idle_nodes'],
                        'free_procs' : v['idle_nodes'] * backdoor_stampede_queue_proc_per_node[k],
                        'num_queueing_jobs' : 0,
                        'num_running_jobs' : 0,
                    }
            for l in stdout.splitlines()[1:]:
                if l.strip(): # if not an empty line
                    jobid, partition, state, nodes = l.strip().split()
                    if 'stampede' in self.hostname:
                        if partition in backdoor_stampede_interested_queues:
                            if state == 'PENDING':
                                workload[partition]['num_queueing_jobs'] += 1
                            elif state == 'RUNNING' or state == 'COMPLETING':
                                workload[partition]['num_running_jobs'] += 1
            # tmp
          # print json.dumps(workload, sort_keys=True, indent=4)
            self.workload = workload
            return workload

    def run(self):
        self.throt1 = 0
        while True:
            cmd = self.queue.get()
            if cmd is "close":
                #self.debug('received "close" command')
                self.queue.task_done()
                #self.debug('task done')
                break
            else:
                self.error("Unknown command {0}".format(str(cmd)))
                self.queue.task_done()
                continue

    def connect_to_db(self):
        pass

    @property
    def num_nodes(self):
        return backdoor_cluster_num_nodes[self.hostname]

class CondorAgent(RemoteBundleAgent):
    """Condor Grid remote access bundle agent
    """
    def __init__(self, remote_login, dbs):
        super(CondorAgent, self).__init__()
        self._login_server = remote_login["login_server"]
        self._cluster_type = remote_login["cluster_type"]
        self._category     = type2category(self._cluster_type)
        self._dbs          = dbs
        self._uid          = self._login_server
        self._config       = None
        self._workload     = None

        self.setup_ssh_connection(remote_login)
        self.start_timer()
        self.start_cmd_loop()

    def __del__(self):
        self.close()

    def get_config(self):
        """Return a dict of resource config.
        """
        return self._config

    def get_workload(self):
        """Return a dict of resource workload.
        """
        return self._workload

    def _update_config(self):
        """Push resource config info to db.
        """
        site_node_slot = self._query_site_node_slot()
        if not site_node_slot:
            print "CondorAgent _update_config() Failed!"
            return

        config = {
                "_id"       : self._uid,
                "timestamp" : datetime.datetime.utcnow(),
                "category"  : self._category,
                "num_nodes" : site_node_slot["num_nodes"],
                "site_info" : {}
                }
        for site, info in site_node_slot["site_info"].iteritems():
            config["site_info"][site] = {
                    k : info[k] for k in \
                            ("site_name", "node_list", "num_nodes")
                    }
        # only overwrite local copy when successful
        self._config = config
        print self._config
        return
        self._dbs.update_resource_config(self._config)

    def _update_workload(self):
        """Push resource config info to db.
        """
        site_node_slot = self._query_site_node_slot()
        if not site_node_slot:
            print "CondorAgent _update_workload() Failed!"
            return

        result = self._query_jobs()
        if not result:
            print "CondorAgent _update_workload() Failed!"
            return
        num_running_jobs, num_queueing_jobs = result

        workload = {
                "resource_id"       : self._uid,
                "timestamp"         : datetime.datetime.utcnow(),
                "num_queueing_jobs" : num_queueing_jobs,
                "busy_jobslots"     : site_node_slot["busy_jobslots"],
                "idle_jobslots"     : site_node_slot["idle_jobslots"],
                }
        for site, info in site_node_slot["site_info"].iteritems():
            workload[site] = {
                    k : info[k] for k in \
                            ("busy_jobslots", "idle_jobslots")
                    }
        # only overwrite local copy when successful
        self._workload = workload
        print self._workload
        return
        self._dbs.update_resource_workload(self._workload)

    def _query_site_node_slot(self):
        """Return useful information of site, node, job slot.
        """
        exit_status, stdout, stderr = self.run_cmd(
                "condor_status -pool osg-flock.grid.iu.edu -state -format '%s' GLIDEIN_Site -format ' %s' Machine -format ' %s\n' State | sort | uniq -c"
                )

        if exit_status != 0:
            print "CondorAgent _query_site_node_slot failed:\n{}\n{}\n{}".format(
                    exit_status, stdout, stderr)
            return None
        else:
            try:
                site_node_slot = {
                        "num_nodes"     : 0,
                        "busy_jobslots" : 0,
                        "idle_jobslots" : 0,
                        "site_info"     : {},
                        }

                for l in stdout.splitlines():
                    l_token = l.strip().split()
                    if len(l_token) == 4:
                        _count, _site, _node, _state = l_token
                        _count = int(_count)

                        if _site not in site_node_slot["site_info"]:
                            site_node_slot["site_info"][_site] = {
                                    "site_name"  : _site,
                                    "node_list"  : [],
                                    "num_nodes"  : 0, # == len(node_list)
                                    "busy_jobslots" : 0,
                                    "idle_jobslots" : 0,
                                    }
                        _site_info = site_node_slot["site_info"][_site]
                        if _node not in _site_info["node_list"]:
                            _site_info["node_list"].append(_node)
                            _site_info["num_nodes"]     += 1
                            site_node_slot["num_nodes"] += 1

                        if _state == "Unclaimed":
                            site_node_slot["idle_jobslots"] += _count
                            _site_info["idle_jobslots"]     += _count
                        elif _state == "Claimed":
                            site_node_slot["busy_jobslots"] += _count
                            _site_info["busy_jobslots"]     += _count
                        elif _state == "Preempting":
                            pass
                        else:
                            # raise BundleException("Unknown State {}: {}".format(_state, l))
                            print "Unknown State {}: {}".format(_state, l)

                return site_node_slot
            except Exception as e:
                print "CondorAgent _query_site_node_slot failed:\n{}\n{}\n{}\n{}".format(
                        stdout, stderr, str(e.__class__), str(e))
                return None

    def _query_jobs(self):
        """Query the total number of running/idle/held jobs.

            $ condor_status -pool osg-flock.grid.iu.edu -schedd -total
                      TotalRunningJobs      TotalIdleJobs      TotalHeldJobs


               Total             18464             152011               2573

           Return:
               Success - [TotalRunningJobs, TotalIdleJobs]
               Failure - None
        """
        exit_status, stdout, stderr = self.run_cmd(
                "condor_status -pool osg-flock.grid.iu.edu -schedd -total"
                )

        if exit_status != 0:
            print "CondorAgent _query_total_jobs failed:\n{}\n{}\n{}".format(
                    exit_status, stdout, stderr)
            return None
        else:
            try:
                return [int(n) for n in\
                        stdout.splitlines()[-1].strip().split()[1:3]]
            except Exception as e:
                print "CondorAgent _query_total_jobs failed:\n{}\n{}".format(
                        stdout, stderr)
                return None

    def _query_job_slots(self, site=None):
        """Query the total number of busy/idle/retiring machines.

            $ condor_status -pool osg-flock.grid.iu.edu -state -total
                      Machines Owner Unclaimed Claimed Preempting Matched

        Benchmarking         8     0         8       0          0       0
                Busy     15663     0         0   15663          0       0
                Idle       256     0       174      82          0       0
            Retiring        30     0         0      30          0       0

               Total     15957     0       182   15775          0       0

           Return:
               Success - [TotalMachines, BusyMachines, IdleMachines]
               Failure - None
        """
        exit_status, stdout, stderr = self.run_cmd(
                "condor_status -pool osg-flock.grid.iu.edu -state -total"
                )

        if exit_status != 0:
            print "CondorAgent _query_total_machines failed:\n{}\n{}\n{}".format(
                    exit_status, stdout, stderr)
            return None
        else:
            try:
                BusyClaimed   = 0
                IdleUnclaimed = 0
                IdleClaimed   = 0
                for line in stdout.splitlines():
                    line = line.strip()
                    if  line.startswith('Busy'):
                        BusyClaimed = int(line.split()[4])
                    elif line.startswith('Idle'):
                        line_token    = line.split()
                        IdleUnclaimed = int(line_token[3])
                        IdleClaimed   = int(line_token[4])
                return [BusyClaimed+IdleUnclaimed+IdleClaimed,\
                        BusyClaimed+IdleClaimed, IdleUnclaimed]
            except Exception as e:
                print "CondorAgent _query_total_jobs failed:\n{}\n{}".format(
                        stdout, stderr)
                return None


supported_types = {
    "moab"     : MoabAgent,
    "pbs"      : PbsAgent,
    "slurm"    : SlurmAgent,
    "condor"   : CondorAgent,
}

def type2category(Type):
    if Type in ["moab", "pbs", "slurm"]:
        return "hpc"
    elif Type in ["condor"]:
        return "grid"
    else:
        return None

def create(resource_config, db_dir=None, verbosity=0):
    try:
        if resource_config["cluster_type"].lower() == 'moab':
            if resource_config['login_server'].lower().startswith('india'):
                resource_config['h_flag'] = True
            return MoabAgent(
                resource_config,
                verbosity,
                db_dir
                )
        elif resource_config["cluster_type"].lower() == 'pbs':
            return PbsAgent(
                resource_config,
                verbosity,
                db_dir
                )
        elif resource_config["cluster_type"].lower() == 'slurm':
            if resource_config['login_server'].lower().startswith('stampede'):
                return SlurmAgent(
                    resource_config,
                    verbosity,
                    db_dir
                    )
            else:
                logging.error("Unsupported slurm cluster: {}".format(resource_config['login_server']))
        logging.error("Unknown cluster type: {}".format(resource_config["cluster_type"]))
    except Exception as e:
        logging.exception('bundle agent creation failure!')

