# -*- coding: utf-8 -*-
import os 
import glob
import time
import logging
import threading

import radical.utils as ru

from aimes.bundle                 import BundleException
from aimes.bundle.db              import DBException
from aimes.bundle.db              import Session as dbSession
from aimes.bundle.bundle_manager  import BundleManager
from aimes.bundle.agent           import BundleAgent


class Session(object):
    """Session class
    """
    def __init__(self, database_url=None, database_name="AIMES_bundle",
                 config_file=None, uid=None):
        """
        """
        self._database_url          = database_url
        self._database_name         = database_name
        self._config_file           = config_file
        self._uid                   = uid
        self._resource_cfgs         = {} # resource_cfgs has a super-set of bundle agents
        self._endpoints             = {} # a dict of res_name:cfg, for bw monitors
        self._dbs                   = None
        self._dbs_metadata          = None
        # self._dbs_connection_info   = None
        self._agent_list            = {}

        if  not self._database_url:
            self._database_url = os.getenv("AIMES_BUNDLE_DBURL", None)

        if  not self._database_url:
            raise BundleException("no database URL (set AIMES_BUNDLE_DBURL)")

        if  uid is None:
            # create new session
            print "Starting a new AIMES.Bundle server session:"
            print "Step (1 of 3): setup database session               ...",
            try:
                self._uid = ru.generate_id('aimes_bundle.session',
                                  mode=ru.ID_PRIVATE)

                # self._dbs, self._dbs_metadata, self._dbs_connection_info = \
                self._dbs, self._dbs_metadata = \
                        dbSession.new(sid     = self._uid,
                                      db_url  = self._database_url,
                                      db_name = database_name)
            except Exception as e:
                print "Failed"
                logging.exception("{}:{}".format(str(e.__class__), str(e)))
                raise BundleException("Session.__init__: db setup Failed!")
            print "Done"

            print "Step (2 of 3): process resource configuration files ...",
            # load resource config json file, parse it into dict
            if  not self._config_file:
                self._config_file = os.getenv("AIMES_BUNDLE_CONFIG", None)

            if  not self._config_file:
                print "Failed"
                raise BundleException("no resource config file (set AIMES_BUNDLE_CONFIG)")

            # TODO parsing all directory
            # if not config_file:
            #     module_path  = os.path.dirname(os.path.abspath(__file__))
            #     default_cfgs = "{}/configs/*.conf".format(module_path)
            #     config_files = glob.glob(default_cfgs)
            # else:
            #     config_files = [config_file, ]

            # for config_file in config_files:
            #     try:
            #         rcs = self.load_resource_config_file(config_file=config_file)
            #     except Exception as e:
            #         print "skip config file {}: {}".format(str(config_file),
            #                 str(e))
            #         continue
            #     for rc in rcs:
            #         self._resource_list[rc] = rcs[rc]

            try:
                self._resource_cfgs = self.load_resource_config_file_json(config_file=self._config_file)
            except Exception as e:
                print "Failed"
                logging.exception("{}:{}".format(str(e.__class__), str(e)))
                raise BundleException("Session.__init__: parsing resource config file Failed!")

            # push resource_cfgs to db
            self._dbs.add_resource_cfgs(resource_cfgs=self._resource_cfgs)
            print "Done"

            print "Step (3 of 3): start bundle agents                  ..."
            for res_name, cfg in self._resource_cfgs.iteritems():
                if not cfg["bundle_agent"]:
                    # This is an endpoint
                    self._endpoints[res_name] = cfg
                    self._endpoints[res_name]["lock"] = threading.Lock()
                    continue

                print "adding {}".format(res_name)

                if self.add_agent(res_name, cfg) != None:
                    print "{} added".format(res_name)
                else:
                    print "{} not added".format(res_name)

            print "Step (3 of 3): start bundle agents                  ...",
            print "Done"

            print "Connect to this Session using:"
            print "  db_url        = ", self._database_url
            print "  database_name = ", self._database_name
            print "  session_id    = ", self._uid

        else:
            # TODO reconnect to existing session
            return

    def close(self, cleanup=True, terminate=True, delete=None):
        print "Closing session ..."
        for uid in self._agent_list.keys():
            self.remove_agent(uid=uid)

    @property
    def version(self):
        return "0.1.0"

    @property
    def service_address(self):
        return "{}/{}.{}".format(self._database_url, self._databasename,
                                 self._uid)

    def load_resource_config_file_json(self, config_file):
        """Load all resource logins from one config file to a dict.
        """
        _valid_p     = ['cluster_type', 'login_server', 'username',
                        'password', 'key_file']
        rcfgs = {}

        try:
            rcf_dict = ru.read_json_str(config_file)

            for res_name, cfg in rcf_dict.iteritems():

                invalid_cfg = False
                for k in ["bundle_agent", "ssh"]:
                    if k not in cfg:
                        invalid_cfg = True
                if invalid_cfg is True:
                    continue

                if cfg["bundle_agent"] is True:
                    if "cluster_type" not in cfg:
                        continue
                    elif cfg["cluster_type"] not in BundleAgent.supported_types:
                        continue
                    else:
                        for k in ["login_server", "username"]:
                            if k not in cfg["ssh"]:
                                invalid_cfg=True
                if invalid_cfg is True:
                    continue

                rcfgs[res_name] = cfg

        except ValueError, err:
            raise BadParameter("Couldn't parse resource configuration file '%s': %s." % (filename, str(err)))

        return rcfgs

    def add_resource(self, rc):
        """Add a new resource to the current session
        """
        pass

    def add_agent(self, uid, cfg):
        """Create a new BundleAgent instance
        """
        if  uid in self._agent_list:
            print "Failed! BundleAgent (uid={}) already exists - won't add a new one. Try remove it first.".format(uid)
            return None

        a = BundleAgent.create(uid=uid, cfg=cfg, dbs=self._dbs)

        if a != None:
            self._agent_list[uid] = a
            return self._agent_list[uid]
        else:
            return None

    def remove_agent(self, uid):
        """Stop/Delete BundleAgent instance
        """
        if  uid not in self._agent_list:
            print "BundleAgent {} doesn't exists".format(uid)
            return
        self._agent_list[uid].close()
        del self._agent_list[uid]

    def publish_pyro_uri(self, uri):
        self._dbs.update_pyro_uri(uri)

    def add_network_monitor(self, agent_id, tgt, frequency=3600):
        # ask the agent to remember the cfg of this tgt endpoint
        self._agent_list[agent_id].add_bw_endpoint(tgt=tgt, cfg=self._endpoints[tgt])
        # do a on-demand query
        self._agent_list[agent_id].measure_bandwidth(tgt)
