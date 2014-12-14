#!/usr/bin/env python

import os
import sys
import time
import logging
import radical.utils as ru

try:
    import bundle_agent
except ImportError:
    logging.critical("can't find module: bundle_agent")
    sys.exit(1)

def ip2id (ip) :
    return ip.replace ('.', '_')

class BundleManager(ru.Daemon):

    def __init__(self):

        # initialize the daemon class
        super (BundleManager, self).__init__ ()

        self.verbose             = 2
        self.cluster_list        = {}
        self.cluster_credentials = []
        self.default_db_dir      = 'db'
        self.configs             = None

        self.bandwidths = dict()
        bw_cfg = '%s/etc/aimes_bundle_bandwidth.cfg' % sys.prefix
        
        with open (bw_cfg, 'r') as f_read :
            for line in f_read :

                line = line.strip ()

                if  line and not line.startswith ('#') :

                    src, dst, bw = line.split (',')

                    if  not src in self.bandwidths :
                        self.bandwidths[src]  = dict()

                    self.bandwidths[src][dst] = bw


    # --------------------------------------------------------------------------
    #
    def start_daemon (self, config_file, mongodb_url) :
        # start daemon process which will execute self.run()

        self.config_file  = config_file
        self.idle_timeout = 60 # FIXME: make configurable
        self.mongodb_url  = mongodb_url

        retval = self.start (debug=False)

        print 'daemonized'
        return retval

    # --------------------------------------------------------------------------
    #
    def run (self) :
        """
        daemon workload

        The daemon will loop forever, sleeping self.idle_timeout seconds after
        each iteration.  Every iteration will query the Bundle Agents for status
        updates, and will push those to the given MongoDB URL.
        """

        try :

            if not self.config_file :
                raise RuntimeError ('no bundle config file -- call run() via start_daemon()!')

            self.load_cluster_credentials(self.config_file)


            while True :

                # FIXME: make configurable via config file
                mongo, db, dbname, cname, pname = ru.mongodb_connect (self.mongodb_url)

                coll_config    = db['config'   ]
                coll_workload  = db['workload' ]
                coll_bandwidth = db['bandwidth']

                ret = self.get_data ()

              # with open ('/tmp/l', 'w+') as log:
              #     import pprint
              #     pprint.pprint (ret)
              #     log.write ("\n\n%s\n\n" % pprint.pformat (ret))

                for cluster_ip in ret['cluster_list'] :

                    cluster_id = ip2id (cluster_ip)
                
                    config     = ret['cluster_config'   ][cluster_id]
                    workload   = ret['cluster_workload' ][cluster_id]
                    bandwidth  = ret['cluster_bandwidth'][cluster_id]

                    timestamp  = time.time ()

                    if config    : config    ['timestamp'] = timestamp
                    if workload  : workload  ['timestamp'] = timestamp
                    if bandwidth : bandwidth ['timestamp'] = timestamp

                    if config    : config    ['_id'] = cluster_id
                    if workload  : workload  ['_id'] = cluster_id
                    if bandwidth : bandwidth ['_id'] = cluster_id

                    if config    : coll_config   .update ({'_id': cluster_id}, config   , upsert=True)
                    if workload  : coll_workload .update ({'_id': cluster_id}, workload , upsert=True)
                    if bandwidth : coll_bandwidth.update ({'_id': cluster_id}, bandwidth, upsert=True)

                time.sleep (self.idle_timeout)

        except Exception as e :
            # FIXME: need a logfile from daemon base class
            raise


    def add_cluster(self, cluster_credential, finished_job_trace):
        if cluster_credential['hostname'] in self.cluster_list:
            #logging.debug("Cluster hostname '{}' already added, will skip it.".format(cluster_credential['hostname']))
            return cluster_credential['hostname']
        cluster = bundle_agent.create(cluster_credential, finished_job_trace, verbosity=self.verbose)
        if cluster:
            self.cluster_list[cluster_credential['hostname']] = cluster
            self.cluster_credentials.append(dict(cluster_credential))
            return cluster_credential['hostname']
        return None

    def remove_cluster(self, cluster_id):
        if cluster_id in self.cluster_list:
            self.cluster_list[cluster_id].close()
            self.cluster_list.pop(cluster_id, None)
        else:
            #logging.debug("Cluster hostname '{}' doesn't exist".format(cluster_id))
            pass

    def get_cluster_list(self):
        return self.cluster_list.keys()

    def get_cluster_configuration(self, cluster_id):
        if cluster_id in self.cluster_list:
            return self.cluster_list[cluster_id].get_configuration()
        else:
            #logging.debug("Cluster hostname '{}' doesn't exist".format(cluster_id))
            return None

    def get_cluster_workload(self, cluster_id):
        if cluster_id in self.cluster_list:
            return self.cluster_list[cluster_id].get_workload()
        else:
            #logging.debug("Cluster hostname '{}' doesn't exist".format(cluster_id))
            return None

    def get_cluster_bandwidth(self, cluster):

        ret = dict()

        for src_ip in self.bandwidths :
            src_id = ip2id (src_ip)

            for tgt_ip in self.bandwidths[src_ip] :
                tgt_id = ip2id (tgt_ip)

                if  src_ip == cluster :
                    if  not tgt_id in ret :
                        ret[tgt_id] = dict()

                    ret[tgt_id]['out'] = self.bandwidths[src_ip][tgt_ip]

                elif tgt_id == cluster :
                    if  not src_id in ret :
                        ret[src_id] = dict()

                    ret[src_id]['in'] = self.bandwidths[src_ip][tgt_id]

        return ret


    def load_cluster_credentials(self, file_name):
        _valid_p = ['cluster_type', 'hostname', 'username', 'password', 'key_filename']
        _mandatory_p = ['cluster_type', 'hostname', 'username']
        _missing_key = False
        try:
            _cred = []
            _db_dir = self.default_db_dir
            _file = open(file_name, 'r')
            for _l in _file:
                _l = _l.strip()
                if _l and not _l.startswith('#'):
                    if _l.startswith('db_dir'):
                        _, _db_dir = _l.split('=')
                        _db_dir = _db_dir.strip()
                    else:
                        _c = {}
                        for _p in _l.split():
                            _k, _v = _p.split('=')
                            _k = _k.strip()
                            if _k not in _valid_p:
                                logging.error("Invalid key '{}' from line '{}'".format(_k, _l))
                                continue
                            _c[_k.strip()] = _v.strip()
                        _missing_key = False
                        for _k in _mandatory_p:
                            if _k not in _c:
                                logging.error("Missing mandatory key '{}' in line '{}'".format(_k, _l))
                                _missing_key = True
                                break
                        if not _missing_key:
                            if _c['cluster_type'] not in bundle_agent.supported_cluster_types:
                                logging.error("Unsupported cluster_type '{}' in line '{}'".format(_c['cluster_type'], _l))
                                continue
                            _cred.append(_c)
            for _c in _cred:
                self.add_cluster(_c, _db_dir)
        except Exception as e:
            logging.exception('')

    def set_verbosity(self, verbose=0):
        self.verbose=verbose
        for c in self.cluster_list.values():
            c.set_verbosity(verbose)

    # --------------------------------------------------------------------------
    # 
    def get_data (self, origin=None) :

        ret = dict()

        ret['cluster_list']      = self.get_cluster_list()
        ret['cluster_config']    = dict()
        ret['cluster_workload']  = dict()
        ret['cluster_bandwidth'] = dict()

        for cluster_ip in ret['cluster_list']:
            cluster_id = ip2id (cluster_ip)
            ret['cluster_config'   ][cluster_id] = self.get_cluster_configuration (cluster_ip)
            ret['cluster_workload' ][cluster_id] = self.get_cluster_workload      (cluster_ip)
            ret['cluster_bandwidth'][cluster_id] = self.get_cluster_bandwidth     (cluster_ip)

        return ret

# --------------------------------------------------------------------------

