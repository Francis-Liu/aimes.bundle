
import os 
import glob
import logging

from aimes.bundle.controller import BundleAgentController
from bundle_manager import BundleManager
# from radical.pilot.resource_config import ResourceConfig
# from ? import BundleException

import radical.utils as ru
from radical.pilot.db import Session as dbSession
from radical.pilot.db import DBException


class Session ():

    #---------------------------------------------------------------------------
    #
    def __init__ (self, database_url=None, database_name="aimesbundle",
                  uid=None, name=None):

        self._database_url  = database_url
        self._database_name = database_name
        self._resource_configs = {}

        # if  not self._database_url :
        #     self._database_url = os.getenv ("AIMES_BUNDLE_DBURL", None)

        # if  not self._database_url :
        #     raise BundleException ("no database URL (set AIMES_BUNDLE_DBURL)")

        # connect to database
        # self._dbs = dbSession.new(sid = self._uid,
        #                           db_url = self._database_url,
        #                           db_name = database_name)

        ##########################
        ## CREATE A NEW SESSION ##
        ##########################
        # load resource config file, parse it into dict
        config_file = os.getenv("AIMES_BUNDLE_CONFIG", None)
        if not config_file:
            module_path = os.path.dirname(os.path.abspath(__file__))
            default_cfgs  = "%s/configs/*.conf" % module_path
            config_files  = glob.glob(default_cfgs)
        else:
            config_files = [config_file, ]

        for config_file in config_files:
            rcs = self.load_cluster_credentials(config_file=config_file)
            for rc in rcs:
                # TODO merging conflicts
                self._resource_configs[rc] = rcs[rc]

        self._controller = BundleAgentController(session=self, db_connection=self._database_url)
        # add each resource to bundle_agent_controller
        for rc in self._resource_configs:
            print "adding {}".format(rc)
            self._controller.add_cluster(self._resource_configs[rc])
            print "{} added".format(rc)

        ######################################
        ## RECONNECT TO AN EXISTING SESSION ##
        ######################################
        return


    #---------------------------------------------------------------------------
    #
    def __del__ (self) :
        self.close ()


    #---------------------------------------------------------------------------
    #
    def close(self, cleanup=True, terminate=True, delete=None):
        return


    #---------------------------------------------------------------------------
    #
    def list_bundle_managers(self):
        return


    # --------------------------------------------------------------------------
    #
    def get_bundle_managers(self, pilot_manager_ids=None) :
        return


    # -------------------------------------------------------------------------
    #
    def add_resource_config(self, resource_config):
        return


    # -------------------------------------------------------------------------
    #
    def get_resource_config (self, resource_key, schema=None):
        return


    # -------------------------------------------------------------------------
    #
    def load_cluster_credentials(self, config_file):
        # TODO load_config_file, json, one org per file
        _valid_p = ['cluster_type', 'hostname', 'username', 'password', 'key_filename']
        _mandatory_p = ['cluster_type', 'hostname', 'username']
        _missing_key = False
        try:
            _cred = {}
            _file = open(config_file, 'r')
            for _l in _file:
                _l = _l.strip()
                if _l and not _l.startswith('#'):
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
                        # if _c['cluster_type'] not in bundle_agent.supported_cluster_types:
                        #     logging.error("Unsupported cluster_type '{}' in line '{}'".format(_c['cluster_type'], _l))
                        #     continue
                        _cred[_c['hostname']] = _c
        except Exception as e:
            logging.exception('')
        finally:
            return _cred


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

