#!/usr/bin/env python

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

try:
    import paramiko
except ImportError:
    logging.critical("Bundle Agent depends on paramiko package installation.")

class BundleAgent(object):
    def get_workload(self):
        raise NotImplementedError()

    def get_configuration(self):
        raise NotImplementedError()

    def set_verbosity(self, verbose):
       self.debug_level = verbose
       if verbose == 0:
           self.logger.setLevel(logging.ERROR)
       elif verbose == 1:
           self.logger.setLevel(logging.INFO)
       elif verbose > 1:
           self.logger.setLevel(logging.DEBUG)

    def setup_logger(self, hostname, verbose):
        self.logger = logging.getLogger(hostname)
        if not self.logger.handlers:
            fmt = '%(asctime)-15s {} %(name)-8s %(levelname)-8s %(funcName)s:%(lineno)-4s %(message)s'.format(self.hostname)
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

class RemoteBundleAgent(BundleAgent):
    def setup_ssh_connection(self, credential):
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh.connect( hostname=self.hostname,
                          port=credential.get('port', 22),
                          username=credential.get('username'),
                          password=credential.get('password'),
                          key_filename=credential.get('key_filename') )
        self.cmd_prefix = ''
        #self.debug("connected to {}".format(self.hostname))
        self.state = 'Up'

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
            self.error("Timeout executing '%s' after %d seconds" % (cmd, timeout))
            chan.close()
            raise
        else:
            return exit_status, stdout, stderr

    def close(self):
        #self.debug('close')
        if self.queue:
            self.queue.put("close")
            self.queue.join()
            self.queue = None
        if self.ssh:
            self.ssh.close()
            self.ssh = None

class MoabAgent(RemoteBundleAgent, threading.Thread):
    #Cluster attributes
    my_type = 'moab'
    torque_version = ''
    moab_version = ''
    alive_nodes = 0
    alive_procs = 0
    busy_nodes = 0
    busy_procs = 0
    free_nodes = 0
    free_procs = 0
    # queue
    # | name | Max Wallclock Limit | HARD MAXPROC constraint
        # Up Nodes | Free Nodes | Processors per Node (ncpus) | Up Processors |
        # Free Processors | Started Flag | Enabled Flag |
    # Only display queues that are non-acl_user_enable
    queue_info = {}
    cmd_prefix = ''
    # Mark this cluster as heterogenerous, needs check per node np, different
    # queue may have different set of nodes.
    h_flag = False
    default_queue = None
    default_pool = None
    properties_nodes = {}
    pbsnodes_list = {}

    #Agent attributes
    purgetime = 300
    sampling_interval = 270 #completed job 5:00
    max_walltime_all = 0
    state_reason_code = ""
    queue = None
    ssh = None
    _timer = None
    job_trace = None
    debug_level = 0

    def __init__(self, credential, verbose=0, db_dir=None):
        self.hostname = credential['hostname']
        self.state = 'INIT'

        #Setup the logger
        self.logger = logging.getLogger(self.hostname)
        if not self.logger.handlers:
            fmt = '%(asctime)-15s {} %(name)-8s %(levelname)-8s %(funcName)s:%(lineno)-4s %(message)s'.format(self.hostname)
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

        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            self.ssh.connect( hostname=self.hostname,
                              port=credential.get('port', 22),
                              username=credential.get('username'),
                              password=credential.get('password'),
                              key_filename=credential.get('key_filename') )
        except Exception as e:
            self.error(str(e.__class__) + str(e))
            raise
        #else:
            #self.debug("Connected to {}".format(self.hostname))

        if credential.get('h_flag') == 'True':
            self.h_flag = True

        #Check if torque/moab is running on this cluster
        exit_status1, stdout1, stderr1 = self.run_cmd("qstat")
        exit_status2, stdout2, stderr2 = self.run_cmd("showq")

        if exit_status1 != 0 or exit_status2 != 0:
            exit_status, stdout, stderr = self.run_cmd("module list")
            if 'command not found' in stderr:
                exit_status, stdout, stderr = self.run_cmd("source /etc/profile.d/modules.sh; module list")
                if 'No such file' in stderr or 'command not found' in stderr:
                    err_msg = "Unable to locate 'module' on {}: {}".format(self.hostname, stderr)
                    self.error(err_msg)
                    raise Exception(err_msg)
                else:
                    self.cmd_prefix += "source /etc/profile.d/modules.sh > /dev/null 2>&1; "
            #module ready

            if 'torque' not in stdout:
                exit_status, stdout, stderr = self.run_cmd("module load torque")
                #if 'Unable to locate' in stderr and 'torque' in stderr:
                if exit_status != 0:
                    err_msg = "Unable to locate 'torque' on {}: {}".format(self.hostname, stderr)
                    self.error(err_msg)
                    raise Exception(err_msg)
                else:
                    self.cmd_prefix += "module load torque > /dev/null 2>&1; "
            #torque ready

            exit_status, stdout, stderr = self.run_cmd("module list")
            if 'moab' not in stdout:
                exit_status, stdout, stderr = self.run_cmd("module load moab")
                #if 'Unable to locate' in stderr and 'moab' in stderr:
                if exit_status != 0:
                    err_msg = "Unable to locate 'moab' on {}: {}".format(self.hostname, stderr)
                    self.error(err_msg)
                    raise Exception(err_msg)
                else:
                    self.cmd_prefix += "module load moab > /dev/null 2>&1; "
            #moab ready

        self.state = 'Up'

        #Run qstat once to get queue configuration
        self.get_queue_info()

        #Run showq once to get # of node/proc(s)
        self.update_node_proc_num()

        # self.load_runtime_history(db_dir)

        if False:
            _ts = time.time()
            _st = datetime.datetime.fromtimestamp(_ts).strftime('%Y-%m-%d_%Hh%Mm%Ss.%f')
            job_trace_filename = 'job_trace_{}_{}'.format(self.hostname, _st)
            self.job_trace = open(os.path.join(db_dir, job_trace_filename), "w")
            self.job_trace.write('{}\n'.format(self.hostname))
            self.exec_showq3()
        #Start sampling thread
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
        #self.debug("Delete moab cluster on {0}".format(self.hostname))
        self.close()

    def cluster_type(self):
        return self.my_type

    def cluster_state(self):
        return self.state

    def num_procs(self):
        return self.alive_procs

    def num_nodes(self):
        return self.alive_nodes

    def workload(self):
        self.update_node_proc_num()
        if self.h_flag is True:
            return {
                'alive_nodes' : self.alive_nodes,
                'alive_procs' : self.alive_procs,
                'busy_nodes' : self.busy_nodes,
                'busy_procs' : self.busy_procs,
                'free_nodes' : self.free_nodes,
                'free_procs' : self.free_procs,
                'per_pool_workload' : self.properties_nodes
                }
        else:
            return {
                'alive_nodes' : self.alive_nodes,
                'alive_procs' : self.alive_procs,
                'busy_nodes' : self.busy_nodes,
                'busy_procs' : self.busy_procs,
                'free_nodes' : self.free_nodes,
                'free_procs' : self.free_procs
                }

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
            self.error("Timeout executing '%s' after %d seconds" % (cmd, timeout))
            chan.close()
            raise
        else:
            return exit_status, stdout, stderr

    # global status
    def exec_showq1(self):
        exit_status, stdout, stderr = self.run_cmd("showq --xml")
        if exit_status != 0:
            self.state = 'Fault'
            self.state_reason_code = stderr
            err_msg = "'showq --xml' return {}: {}".format(exit_status, stderr)
            self.error(err_msg)
            raise Exception(err_msg)
        elif self.state is 'Fault':
            self.state = 'Up'
            self.state_reason_code = ''

        root = ET.fromstring(stdout)
        cluster = root.find('cluster')
        self.alive_nodes = int(cluster.attrib['LocalUpNodes'])
        self.alive_procs = int(cluster.attrib['LocalUpProcs'])
        self.busy_nodes = int(cluster.attrib['LocalActiveNodes'])
        self.busy_procs = int(cluster.attrib['LocalAllocProcs'])
        self.free_nodes = int(cluster.attrib['LocalIdleNodes'])
        self.free_procs = int(cluster.attrib['LocalIdleProcs'])
        return self.alive_nodes, self.alive_procs, self.busy_nodes, \
               self.busy_procs, self.free_nodes, self.free_procs

    # running/queueing list
    def exec_showq2(self, pool=None):
        exit_status, stdout, stderr = self.run_cmd("showq --xml")
        if exit_status != 0:
            err_msg = "'showq --xml' return ({}): stdout({}), stderr({})".format(
                exit_status, stdout, stderr)
            self.error(err_msg)
            raise Exception(err_msg)

        root = ET.fromstring(stdout)
        cluster = root.find('cluster')
        cur_time = int(cluster.attrib['time'])
        cur_alive_nodes = int(cluster.attrib['LocalUpNodes'])
        cur_busy_nodes = int(cluster.attrib['LocalActiveNodes'])
        cur_avail_nodes = int(cluster.attrib['LocalIdleNodes'])

        running_list = []
        queueing_list = []
        for queue in root.iter('queue'):
            if queue.attrib['option'] == 'active':
                for job in queue.findall('job'):
                    if pool is not None and self.queue_features[job.attrib['Class']] != pool:
                        continue
                    #temp solution for clusters don't report ReqNodes
                    if 'ReqNodes' in job.attrib:
                        ReqNodes = int(job.attrib['ReqNodes'])
                    else:
                        ReqNodes = int(job.attrib['ReqProcs']) / 8 + (1 if int(job.attrib['ReqProcs']) % 8 else 0)
                    running_list.append( {
                        'JobID':job.attrib['JobID'],
                        'Class':job.attrib['Class'],
                        'Group':job.attrib['Group'],
                        'User':job.attrib['User'],
                        'JobName':job.attrib['JobName'],
                        'ReqNodes':ReqNodes,
                        'ReqProcs':int(job.attrib['ReqProcs']),
                        'StartTime':int(job.attrib['StartTime']),
                        'WallTime':int(job.attrib['ReqAWDuration'])
                        }
                    )
            elif queue.attrib['option'] == 'eligible':
                for job in queue.findall('job'):
                    if pool is not None and self.queue_features[job.attrib['Class']] != pool:
                        continue
                    if job.attrib['JobName'] == 'poweroff':
                        continue
                    #temp solution for clusters don't report ReqNodes
                    if 'ReqNodes' in job.attrib:
                        ReqNodes = int(job.attrib['ReqNodes'])
                    else:
                        ReqNodes = int(job.attrib['ReqProcs']) / 8 + (1 if int(job.attrib['ReqProcs']) % 8 else 0)
                    queueing_list.append( {
                        'JobID':job.attrib['JobID'],
                        'Class':job.attrib['Class'],
                        'Group':job.attrib['Group'],
                        'User':job.attrib['User'],
                        'JobName':job.attrib['JobName'],
                        'ReqNodes':ReqNodes,
                        'ReqProcs':int(job.attrib['ReqProcs']),
                        'WallTime':int(job.attrib['ReqAWDuration']),
                        'Priority':int(job.attrib['StartPriority'])
                        }
                    )
        return cur_time, cur_alive_nodes, cur_busy_nodes, cur_avail_nodes, running_list, queueing_list

    # record completed jobs
    def exec_showq3(self):
        exit_status, stdout, stderr = self.run_cmd("showq -c --xml")
        if exit_status != 0:
            self.state = 'Fault'
            self.state_reason_code = stderr
            err_msg = "showq -c --xml: " + stderr
            self.error(err_msg)
            raise Exception(err_msg)
        elif self.state is 'Fault':
            self.state = 'Up'
            self.state_reason_code = ''

        root = ET.fromstring(stdout)
        for queue in root.iter('queue'):
            if 'option' not in queue.attrib:
                pass
            elif queue.attrib['option'] == 'completed':
                _purgetime = int(queue.attrib['purgetime'])
                if self.purgetime != _purgetime:
                    self.warn('purgetime changed from {} to {}'.format(self.purgetime, _purgetime))
                    if _purgetime < 30:
                        self.warn("purgetime {} is too short! Can't set an approporiate sampling time".format(_purgetime))
                    else:
                        self.purgetime = _purgetime
                        sampling_interval = _purgetime*9/10
                for job in queue.findall('job'):
                    _JobID = job.attrib['JobID']
                    if _JobID not in self.rtime_tbl:
                        #begin critical area
                        self.rtime_tbl[_JobID] = [int(job.attrib['AWDuration']), int(job.attrib['CompletionTime'])]
                        if 'ReqNodes' in job.attrib:
                            _job_signature = ( job.attrib['Class'],
                                               job.attrib['Group'],
                                               job.attrib['User'],
                                               job.attrib['JobName'],
                                               int(job.attrib['ReqNodes']),
                                               int(job.attrib['ReqProcs']),
                                               int(job.attrib['ReqAWDuration']) )
                        else:
                            _job_signature = ( job.attrib['Class'],
                                               job.attrib['Group'],
                                               job.attrib['User'],
                                               job.attrib['JobName'],
                                               int(job.attrib['ReqProcs']),
                                               int(job.attrib['ReqAWDuration']) )
                        if _job_signature in self.rtime_lkup_tbl:
                            if int(job.attrib['CompletionTime']) > self.rtime_tbl[self.rtime_lkup_tbl[_job_signature][0]][1]:
                                self.rtime_lkup_tbl[_job_signature][1] = self.rtime_lkup_tbl[_job_signature][0]
                                self.rtime_lkup_tbl[_job_signature][0] = _JobID
                            elif self.rtime_lkup_tbl[_job_signature][1] != -1 and \
                                int(job.attrib['CompletionTime']) > self.rtime_tbl[self.rtime_lkup_tbl[_job_signature][1]][1]:
                                self.rtime_lkup_tbl[_job_signature][1] = _JobID
                        else:
                            self.rtime_lkup_tbl[_job_signature] = [_JobID, -1]
                        #self.debug('found newly completed job {} - {}: {}'.format(_JobID, _job_signature, self.rtime_tbl[_JobID]))
                        if 'ReqNodes' in job.attrib:
                            self.job_trace.write('JobID={} JobName={} DRMJID={} Class={} Group={} User={} ReqNodes={} ReqProcs={} ReqAWDuration={} State={} CompletionCode={} SubmissionTime={} StartTime={} CompletionTime={} SuspendDuration={} AWDuration={}\n'.format(
                                job.attrib['JobID'], job.attrib['JobName'], job.attrib['DRMJID'], job.attrib['Class'], job.attrib['Group'], job.attrib['User'], job.attrib['ReqNodes'], job.attrib['ReqProcs'], job.attrib['ReqAWDuration'], job.attrib['State'], job.attrib['CompletionCode'], job.attrib['SubmissionTime'], job.attrib['StartTime'], job.attrib['CompletionTime'], job.attrib['SuspendDuration'], job.attrib['AWDuration']))
                        else:
                            self.job_trace.write('JobID={} JobName={} DRMJID={} Class={} Group={} User={} ReqProcs={} ReqAWDuration={} State={} CompletionCode={} SubmissionTime={} StartTime={} CompletionTime={} SuspendDuration={} AWDuration={}\n'.format(
                                job.attrib['JobID'], job.attrib['JobName'], job.attrib['DRMJID'], job.attrib['Class'], job.attrib['Group'], job.attrib['User'], job.attrib['ReqProcs'], job.attrib['ReqAWDuration'], job.attrib['State'], job.attrib['CompletionCode'], job.attrib['SubmissionTime'], job.attrib['StartTime'], job.attrib['CompletionTime'], job.attrib['SuspendDuration'], job.attrib['AWDuration']))
                        self.job_trace.flush()
                        #end critical area

    # run pbsnodes -a to collect info of node status
    def pbsnodes(self):
        exit_status, stdout, stderr = self.run_cmd("pbsnodes -a -x")
        if exit_status != 0:
            err_msg = "'pbsnodes -a -x' return ({}): stdout({}), stderr({})".format(
                exit_status, stdout, stderr)
            self.error(err_msg)
            raise Exception(err_msg)
        root = ET.fromstring(stdout)
        node_list={}
        for node in root.findall('Node'):
            #if state == 'free' or state == 'job-exclusive':
            node_list[node.find('name').text] = [
                node.find('state').text,
                int(node.find('np').text),
                node.find('properties').text,
                node.find('jobs').text if node.find('jobs') != None else None
                ]
        return node_list

    def update_node_proc_num(self, pool=None):
        if self.h_flag is False:
            return self.exec_showq1()
        else:
            nl = self.pbsnodes()
            properties_nodes = {}
            for k, v in nl.items():
                state, np, properties, jobs = v
                if properties in self.queue_features.values():
                    if properties not in properties_nodes:
                        properties_nodes[properties] = {
                            'np' : np,
                            'alive_nodes' : 0,
                            'alive_procs' : 0,
                            'busy_nodes' : 0,
                            'busy_procs' : 0,
                            'free_nodes' : 0,
                            'free_procs' : 0
                            }
                    if state == 'free' and jobs == None:
                        properties_nodes[properties]['free_nodes'] += 1
                        properties_nodes[properties]['free_procs'] += np
                        properties_nodes[properties]['alive_nodes'] += 1
                        properties_nodes[properties]['alive_procs'] += np
                    elif state == 'job-exclusive' or \
                        (state == 'free' and jobs != None):
                        properties_nodes[properties]['busy_nodes'] += 1
                        # TODO fine-grained count partially free procs
                        properties_nodes[properties]['busy_procs'] += np
                        properties_nodes[properties]['alive_nodes'] += 1
                        properties_nodes[properties]['alive_procs'] += np
            self.pbsnodes_list = nl
            self.properties_nodes = properties_nodes
            self.free_nodes = sum([v['free_nodes'] for k, v in properties_nodes.items()])
            self.free_procs = sum([v['free_procs'] for k, v in properties_nodes.items()])
            self.busy_nodes = sum([v['busy_nodes'] for k, v in properties_nodes.items()])
            self.busy_procs = sum([v['busy_procs'] for k, v in properties_nodes.items()])
            self.alive_nodes = self.free_nodes + self.busy_nodes
            self.alive_procs = self.free_procs + self.busy_procs
            if pool is None:
                return self.alive_nodes, self.alive_procs, \
                    self.busy_nodes, self.busy_procs, \
                    self.free_nodes, self.free_procs
            else:
                return properties_nodes[pool]['alive_nodes'], \
                    properties_nodes[pool]['alive_procs'], \
                    properties_nodes[pool]['busy_nodes'], \
                    properties_nodes[pool]['busy_procs'], \
                    properties_nodes[pool]['free_nodes'], \
                    properties_nodes[pool]['free_procs']

    def get_queue_feature(self, queue_info):
        exit_status, stdout, stderr = self.run_cmd("showconfig -v")
        queue_features = {}
        r = re.compile('[ \[\]=]+')
        for line_str in stdout.splitlines():
            if line_str.find('CLASSCFG') == 0:
                (_,q,_,f,_)=r.split(line_str)
                if q in queue_info: # Not self.queue_info!
                    queue_features[q] = f
        self.queue_features = queue_features

    def get_queue_info(self):
        exit_status, stdout, stderr = self.run_cmd("qstat -Q -f")
        #TODO handle failure case
        _queue_info = {}
        for line_str in stdout.splitlines():
            if line_str.find('Queue') == 0:
                pos = line_str.find(':')
                queue_name = line_str[pos+1:].strip()
                _queue_info[queue_name] = {'queue_name' : queue_name}
            elif line_str.find('=') != -1:
                queue_attr_k = line_str.lstrip()[:line_str.lstrip().find(' ')]
                pos1 = line_str.find('=')
                queue_attr_v = line_str[pos1+1:].strip()
                if queue_attr_k == 'acl_user_enable':
                    if (queue_attr_v == 'True'):
                        _queue_info[queue_name]['restricted_access'] = True
                elif queue_attr_k == 'resources_max.walltime':
                    if queue_attr_v.count(':') == 2:
                        h, m, s = [int(i) for i in queue_attr_v.split(':')]
                        _queue_info[queue_name]['max_walltime'] = 3600*h+60*m+s
                    elif queue_attr_v.count(':') == 3:
                        d, h, m, s = [int(i) for i in queue_attr_v.split(':')]
                        _queue_info[queue_name]['max_walltime'] = 24*3600*d+3600*h+60*m+s
                elif queue_attr_k == 'started' or queue_attr_k == 'enabled':
                    _queue_info[queue_name][queue_attr_k] = queue_attr_v
        for k,v in _queue_info.items():
            if ('restricted_access' in v) and (v['restricted_access'] == True):
                del _queue_info[k]

        if self.h_flag is True:
            self.get_queue_feature(_queue_info)
            #TODO check failure
            for k, v in _queue_info.items():
                _queue_info[k]['pool'] = self.queue_features[k]
            self.get_default_queue()
            #TODO check failure
            self.default_pool = self.queue_features[self.default_queue]

        self.max_walltime_all = max(v['max_walltime'] for v in _queue_info.values() if 'max_walltime' in v)
        #self.debug('{} max_walltime = {}'.format(
        #    self.hostname, 
        #    str(datetime.timedelta(seconds=self.max_walltime_all)))
        #)

        self.queue_info = _queue_info

    def max_walltime(self, pool=None):
        if pool is None:
            return self.max_walltime_all
        else:
            return max(v['max_walltime'] for v in self.queue_info.values() \
                if v['pool'] == pool)

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
        #print self.default_queue
        return self.default_queue

    def get_attributes(self):
        attributes = {}
        attributes['queue_info'] = self.queue_info
        if self.h_flag is True:
            properties_nodes = self.properties_nodes
            pool_size = {}
            for k, v in properties_nodes.items():
                pool_size[k] = {}
                pool_size[k]['np'] = v['np']
                pool_size[k]['num_procs'] = v['alive_procs']
                pool_size[k]['num_nodes'] = v['alive_nodes']
            attributes['pool'] = pool_size
        attributes['num_procs'] = self.num_procs()
        attributes['num_nodes'] = self.num_nodes()
        attributes['state'] = self.cluster_state()
        return attributes

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

    def show_start(self, p_procs, wall_time):
        exit_status, stdout, stderr = self.run_cmd("showstart --xml {0}@{1}".format(
                str(p_procs), wall_time))
        #print "exit_status={} stdout={}".format(exit_status, stdout)
        #TODO handle failure case
        if 'job' not in stdout or 'StartTime' not in stdout or 'now' not in stdout:
            self.error(stdout)
            return -1
        else:
            tree = ET.fromstring(stdout)
            return int(tree.find('job').attrib['StartTime']) - int(tree.find('job').attrib['now'])

    def set_verbosity(self, verbose):
       self.debug_level = verbose
       if verbose == 0:
           self.logger.setLevel(logging.ERROR)
       elif verbose == 1:
           self.logger.setLevel(logging.INFO)
       elif verbose > 1:
           self.logger.setLevel(logging.DEBUG)
       # self.logger.setLevel(logging.ERROR)

    def close(self):
        # self.debug('close')
        if self.queue:
            self.queue.put("close")
            self.queue.join()
            self.queue = None
        if self.ssh:
            self.ssh.close()
            self.ssh = None
        if self.job_trace:
            self.job_trace.close()

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
        self.hostname = credential['hostname']
        self.state = 'INIT'
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

    def cluster_state(self):
        return self.state

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
        self.hostname = credential['hostname']
        self.state = 'INIT'
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

    def cluster_state(self):
        return self.state

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

supported_cluster_types = {
    "moab" : MoabAgent,
    "pbs" : PbsAgent,
    "slurm" : SlurmAgent,
}

def create(cluster_attributes, db_dir=None, verbosity=0):
    try:
        if cluster_attributes['cluster_type'].lower() == 'moab':
            if cluster_attributes['hostname'].lower().startswith('india'):
                cluster_attributes['h_flag'] = True
            return MoabAgent(
                cluster_attributes,
                verbosity,
                db_dir
                )
        elif cluster_attributes['cluster_type'].lower() == 'pbs':
            return PbsAgent(
                cluster_attributes,
                verbosity,
                db_dir
                )
        elif cluster_attributes['cluster_type'].lower() == 'slurm':
            if cluster_attributes['hostname'].lower().startswith('stampede'):
                return SlurmAgent(
                    cluster_attributes,
                    verbosity,
                    db_dir
                    )
            else:
                logging.error("Unsupported slurm cluster: {}".format(cluster_attributes['hostname']))
        logging.error("Unknown cluster type: {}".format(cluster_attributes['type']))
    except Exception as e:
        logging.exception('bundle agent creation failure!')

