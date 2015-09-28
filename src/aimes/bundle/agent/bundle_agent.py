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
import saga

import paramiko

from aimes.bundle import BundleException


class RemoteBundleAgent(threading.Thread):
    def __init__(self, resource_config, dbs):
        self._login_server = resource_config["login_server"]
        self._username     = resource_config["username"]
        self._cluster_type = resource_config["cluster_type"]
        self._category     = type2category(self._cluster_type)
        self._uid          = self._login_server
        self._dbs          = dbs
        self._sampling_interval = 60
        self._config       = None
        self._workload     = None
        self._queue        = Queue.Queue()

        threading.Thread.__init__(self, name=self._uid + " bundle agent")

        self.setup_ssh_connection(resource_config)
        # self.start_bw_server()
        self.start_timer()
        self.start_cmd_loop()

    def set_verbosity(self, verbose):
       self.debug_level = verbose
       if verbose == 0:
           self.logger.setLevel(logging.ERROR)
       elif verbose == 1:
           self.logger.setLevel(logging.INFO)
       elif verbose > 1:
           self.logger.setLevel(logging.DEBUG)

    def setup_logger(self, verbose):
        self.logger = logging.getLogger(self._uid)
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
        self._queue.put("update_config")
        self._queue.put("update_workload")
        self._timer = threading.Timer(self._sampling_interval, self.start_timer)
        self._timer.setDaemon(True)
        self._timer.start()

    def run(self):
        while True:
            cmd = self._queue.get()
            if cmd is "update_config":
                self._update_config()
                self._queue.task_done()
            elif cmd is "update_workload":
                self._update_workload()
                self._queue.task_done()
            elif cmd is "close":
                if self._timer:
                    self._timer.cancel()
                self._queue.task_done()
                break
            else:
                print "Unkown command {}".format(cmd)
                self._queue.task_done()

    def close(self):
        if self._queue:
            self._queue.put("close")
            self._queue.join()
            self._queue = None
        if self.ssh:
            self.ssh.close()
            self.ssh = None

    def get_config(self):
        """Return a dict of resource config.
        """
        return self._config

    def get_workload(self):
        """Return a dict of resource workload.
        """
        return self._workload

    def start_bw_server(self):
        """Run an iperf program in server mode.
        """
        REMOTE_JOB_ENDPOINT  = "ssh://"  + self._login_server
        REMOTE_DIR = "sftp://" + self._login_server + "/tmp/aimes.bundle/iperf/"

        ctx = saga.Context("ssh")
        ctx.user_id = self._username

        session = saga.Session()
        session.add_context(ctx)

        workdir   = saga.filesystem.Directory(REMOTE_DIR, saga.filesystem.CREATE_PARENTS, session=session)
        mbwrapper = saga.filesystem.File(
                'file://localhost/%s/start-iperf-server-daemon.sh' % os.path.dirname(__file__))
        mbwrapper.copy(workdir.get_url())
        mbexe     = saga.filesystem.File(
                'file://localhost/%s/../third_party/iperf-3.0.11-source.tar.gz' % os.path.dirname(__file__))
        mbexe.copy(workdir.get_url())

        js = saga.job.Service(REMOTE_JOB_ENDPOINT, session=session)
        jd = saga.job.Description()

        jd.executable        = "./start-iperf-server-daemon.sh"
        iperf_local_port     = 55201
        jd.arguments         = [iperf_local_port]

        myjob = js.create_job(jd)
        myjob.run()


class MoabAgent(RemoteBundleAgent, threading.Thread): pass

backdoor_cluster_num_nodes = {
    'stampede.tacc.xsede.org'  : 6400, # https://www.tacc.utexas.edu/user-services/user-guides/stampede-user-guide
    'gordon.sdsc.xsede.org'    : 1024, # http://www.sdsc.edu/supercomputing/gordon/using_gordon/allocations.html
    'hopper.sdsc.xsede.org'    : 1024, # https://www.nersc.gov/users/computations-systems/hopper/
    'hopper.nersc.gov'         : 6384, # https://www.nersc.gov/users/computational-systems/hopper/configuration/compute-nodes/
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
    """PBS HPC remote access bundle agent.
    """
    def __init__(self, resource_config, dbs):
        super(PbsAgent, self).__init__(resource_config, dbs)

    def __del__(self):
        self.close()

    @property
    def num_nodes(self):
        return backdoor_cluster_num_nodes[self._login_server]

    def _update_config(self):
        queue_info = self.get_queue_config()
        if not queue_info:
            print "PbsAgent _update_config() failed!"
            return

        config = {
                "_id"        : self._uid,
                "timestamp"  : datetime.datetime.utcnow(),
                "category"   : self._category,
                'num_nodes'  : self.num_nodes,
                'queue_info' : queue_info
        }
        self._config = config
        self._dbs.update_resource_config(self._config)

    def _update_workload(self):
        if 'hopper' in self._login_server:
            return self._update_workload_hopper()

        try:
            self.get_queue_config(1)

            if 'gordon' in self._login_server:
                properties_queues = backdoor_gordon_properties_queues

            nl = self.pbsnodes()
            if not nl:
                print "PbsAgent _update_workload failed"
                return

            workload = {
                    "resource_id"       : self._uid,
                    "timestamp"         : datetime.datetime.utcnow(),
                    }
            for k, v in self.queue_info.items():
                workload[k] = {
                        'alive_nodes'       : 0,
                        'alive_procs'       : 0,
                        'busy_nodes'        : 0,
                        'busy_procs'        : 0,
                        'free_nodes'        : 0,
                        'free_procs'        : 0,
                        'num_queueing_jobs' : v['num_queueing_jobs'],
                        'num_running_jobs'  : v['num_running_jobs'],
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
                        print "unknown node properties: {}".format(properties)
                        continue

            # only overwrite local copy when successful
            self._workload = workload
            self._dbs.update_resource_workload(self._workload)
        except Exception as e:
            logging.exception("PbsAgent _update_workload failed")

    def _update_workload_hopper(self):
        try:
            # self.get_queue_config(1)
            queue_nodes = self.qstat_x()
            if not queue_nodes:
                return

            workload = {
                    "resource_id"       : self._uid,
                    "timestamp"         : datetime.datetime.utcnow(),
                    }
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

        except Exception as e:
            print "PbsAgent get_queue_config failed:\n{}\n{}".format(
                    str(e.__class__), str(e))
            return

        # only overwrite local copy when successful
        self._workload = workload
        self._dbs.update_resource_workload(self._workload)

    def get_queue_config(self, flag=0):
        try:
            exit_status, stdout, stderr = self.run_cmd("qstat -Q -f")
            if exit_status != 0:
                print "'qstat -Q -f' return ({}): stdout({}), stderr({})".format(
                    exit_status, stdout, stderr)
                return None
        except Exception as e:
            logging.exception('PbsAgent get_queue_config run qstat failed')
            return None

        try:
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
                if  ('gordon'     in self._login_server and v['queue_name'] not in backdoor_gordon_interested_queues)     or \
                    ('hopper'     in self._login_server and v['queue_name'] not in backdoor_hopper_interested_queues)     or \
                    (('restricted_access' in v) and (v['restricted_access'] == True)) or \
                    (('enabled'           in v) and (v['enabled']           != True)) or \
                    (('started'           in v) and (v['started']           != True)):
                    del _queue_info[k]
                    continue

                if 'enabled' in v:
                    del _queue_info[k]['enabled']
                if 'started' in v:
                    del _queue_info[k]['started']

            if 'gordon' in self._login_server:
                for k, v in _queue_info.items():
                    v['num_procs_limit'] = backdoor_gordon_queue_num_procs_limits[k]
            elif 'hopper' in self._login_server:
                for k, v in _queue_info.items():
                    v['queued_jobs_limit'] = backdoor_hopper_queue_jobs_limits[k]
                    v['num_procs_limit'] = backdoor_hopper_queue_num_procs_limits[k]
            self.queue_info = _queue_info
            return self.queue_info
        except Exception as e:
            print "PbsAgent get_queue_config failed:\n{}\n{}\n{}\n{}".format(
                    str(e.__class__), str(e), stdout, stderr)
            return None

    # TODO add more error handling, what if the queue is empty?
    def qstat_x(self):
        try:
            exit_status, stdout, stderr = self.run_cmd("qstat -x")
            if exit_status != 0:
                print "'qstat -a -x' return ({}): stdout({}), stderr({})".format(
                    exit_status, stdout, stderr)
                return None

            queue_nodes = {}
            root = ET.fromstring(stdout)
            for job in root.findall('Job'):
                _queue = job.find('queue').text
                if 'hopper' in self._login_server:
                    _interested_queues = backdoor_hopper_interested_queues
                    if 'reg' in _queue:
                        _queue = 'regular'

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
                            if 'hopper' in self._login_server:
                                queue_nodes[_queue]['num_busy_procs'] += int(job.find('Resource_List').find('mppwidth').text)
                    except Exception as e:
                        pass
            return queue_nodes
        except Exception as e:
            logging.exception("PbsAgent qstat_x failed")
            return None

    def pbsnodes(self):
        """Run pbsnodes -a to collect info of node status
        """
        try:
            exit_status, stdout, stderr = self.run_cmd("pbsnodes -a -x")
            if exit_status != 0:
                print "remote run pbanodes failed:\n{}\n{}\n{}".format(
                        exit_status, stdout, stderr)
                return None

            root = ET.fromstring(stdout)
            node_list = {}
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
                    # TODO check error
                    pass

            return node_list
        except Exception as e:
            logging.exception("pbsnodes failed:\n{}\n{}\n{}".format(exit_status, stdout, stderr))
            return None


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

class SlurmAgent(RemoteBundleAgent):
    """Slurm HPC remote access bundle agent.
    """
    def __init__(self, resource_config, dbs):
        super(SlurmAgent, self).__init__(resource_config, dbs)

    def __del__(self):
        self.close()

    @property
    def num_nodes(self):
        return backdoor_cluster_num_nodes[self._login_server]

    def _update_config(self):
        queue_info = self.get_queue_config()
        if not queue_info:
            print "SlurmAgent _update_config() Failed!"
            return

        config = {
                "_id"        : self._uid,
                "timestamp"  : datetime.datetime.utcnow(),
                "category"   : self._category,
                'num_nodes'  : self.num_nodes,
                'queue_info' : queue_info
        }
        self._config = config
        self._dbs.update_resource_config(self._config)

    def _update_workload(self):
        queue_config = self.get_queue_config(1)
        if not queue_config:
            print "SlurmAgent _update_workload failed!"
            return

        try:
            exit_status, stdout, stderr = self.run_cmd('squeue -r -o "%.18i %.13P %.13T %.6D"')
            if exit_status != 0:
                print "SlurmAgent _update_workload call 'squeue' failed:\n{}\n{}\n{}".format(
                    exit_status, stdout, stderr)
                return

            workload = {
                    "resource_id"       : self._uid,
                    "timestamp"         : datetime.datetime.utcnow(),
                    }
            if 'stampede' in self._login_server:
                for k, v in queue_config.items():
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
                    if 'stampede' in self._login_server:
                        if partition in backdoor_stampede_interested_queues:
                            if state == 'PENDING':
                                workload[partition]['num_queueing_jobs'] += 1
                            elif state == 'RUNNING' or state == 'COMPLETING':
                                workload[partition]['num_running_jobs'] += 1
        except Exception as e:
            print "SlurmAgent get_queue_config failed:\n{}\n{}\n{}\n{}".format(
                    str(e.__class__), str(e), stdout, stderr)
            return

        # only overwrite local copy when successful
        self._workload = workload
        self._dbs.update_resource_workload(self._workload)

    def get_queue_config(self, flag=0):
        try:
            exit_status, stdout, stderr = self.run_cmd('sinfo -o "%20P %5a %.10l %20F"')
            if exit_status != 0:
                print "'sinfo' return ({}): stdout({}), stderr({})".format(
                    exit_status, stdout, stderr)
                return None
        except:
            logging.exception("SlurmAgent get_queue_config query sinfo failed")
            return None

        try:
            _queue_info = {}

            for line_str in stdout.splitlines()[1:]:
                _partition, _avail, _timelimit, _nodes = line_str.split()
                _partition=_partition.rstrip('*')
                if flag == 0:
                    r = re.compile('[-:]+')
                    d, h, m, s = [int(i) for i in r.split(_timelimit)]
                    _timelimit = 24*3600*d+3600*h+60*m+s
                    if 'stampede' in self._login_server and _partition in backdoor_stampede_interested_queues:
                        _queue_info[_partition] = {'queue_name' : _partition, 'max_walltime' : _timelimit, \
                            'num_procs_limit' : backdoor_stampede_queue_num_procs_limits[_partition], \
                            'queued_jobs_limit' : backdoor_stampede_queue_jobs_limits[_partition]
                        }
                else:
                    if 'stampede' in self._login_server and _partition in backdoor_stampede_interested_queues:
                        _active_nodes, _idle_nodes, _other_nodes, _total_nodes = [int(n) for n in _nodes.split('/')]
                        _queue_info[_partition] = {'queue_name' : _partition, 'active_nodes': _active_nodes, \
                            'idle_nodes' : _idle_nodes, 'other_nodes' : _other_nodes, 'total_nodes' : _total_nodes }
            return _queue_info
        except Exception as e:
            print "SlurmAgent get_queue_config failed:\n{}\n{}\n{}\n{}".format(
                    str(e.__class__), str(e), stdout, stderr)
            return None


class CondorAgent(RemoteBundleAgent):
    """Condor Grid remote access bundle agent.
    """
    def __init__(self, resource_config, dbs):
        super(CondorAgent, self).__init__(resource_config, dbs)

    def __del__(self):
        self.close()

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
        self._dbs.update_resource_workload(self._workload)

    def _query_site_node_slot(self):
        """Return useful information of site, node, job slot.
        """
        try:
            exit_status, stdout, stderr = self.run_cmd(
                    "condor_status -pool osg-flock.grid.iu.edu -state -format '%s' GLIDEIN_Site -format ' %s' Machine -format ' %s\n' State | sort | uniq -c"
                    )
            if exit_status != 0:
                print "CondorAgent _query_site_node_slot failed:\n{}\n{}\n{}".format(
                        exit_status, stdout, stderr)
                return None

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
                    str(e.__class__), str(e), stdout, stderr)
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
        try:
            exit_status, stdout, stderr = self.run_cmd(
                    "condor_status -pool osg-flock.grid.iu.edu -schedd -total"
                    )

            if exit_status != 0:
                print "CondorAgent _query_total_jobs failed:\n{}\n{}\n{}".format(
                        exit_status, stdout, stderr)
                return None
        except Exception as e:
            logging.exception("CondorAgent _query_jobs run condor_status failed")
            return None

        try:
            return [int(n) for n in\
                    stdout.splitlines()[-1].strip().split()[1:3]]
        except Exception as e:
            logging.exception("CondorAgent _query_jobs failed:\n{}".format(stdout))
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

def create(resource_config, dbs):
    try:
        ct = resource_config["cluster_type"].lower()
        if ct not in supported_types:
            logging.error("Unknown cluster type: {}".format(ct))
            return None
        return supported_types[ct](
                resource_config=resource_config,
                dbs=dbs)
    except Exception as e:
        logging.exception('Bundle agent creat Failed!')
        return None
