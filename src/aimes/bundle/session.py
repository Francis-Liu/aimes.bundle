# -*- coding: utf-8 -*-

import os 
import glob
import logging
import time

from aimes.bundle                 import BundleException
from aimes.bundle.db              import DBException
from aimes.bundle.db              import Session as dbSession
from aimes.bundle.bundle_manager  import BundleManager
# from aimes.bundle                 import resource_bundle
from aimes.bundle.agent           import BundleAgent

import radical.utils      as ru


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
        self._resource_list         = {}
        self._dbs                   = None
        self._dbs_metadata          = None
        self._dbs_connection_info   = None
        self._agent_list            = {}

        if  not self._database_url:
            self._database_url = os.getenv("AIMES_BUNDLE_DBURL", None)

        if  not self._database_url:
            raise BundleException("no database URL (set AIMES_BUNDLE_DBURL)")

        if  uid is None:
            # create new session
            print "Initializing AIMES.Bundle session:"
            print "Step (1 of 4): setup database session               ...",
            try:
                self._uid = ru.generate_id('aimes_bundle.session',
                                  mode=ru.ID_PRIVATE)

                self._dbs, self._dbs_metadata, self._dbs_connection_info = \
                        dbSession.new(sid     = self._uid,
                                      db_url  = self._database_url,
                                      db_name = database_name)
            except Exception as e:
                print "Failed"
                logging.exception("{}:{}".format(str(e.__class__), str(e)))
                raise BundleException("Session.__init__: db setup Failed!")
            print "Success"

            print "Step (2 of 4): process resource configuration files ...",
            # load resource config file, parse it into dict
            if  not self._config_file:
                self._config_file = os.getenv("AIMES_BUNDLE_CONFIG", None)

            if  not self._config_file:
                print "Failed"
                raise BundleException("no resource config file (set AIMES_BUNDLE_CONFIG)")

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
                rcs = self.load_resource_config_file(config_file=self._config_file)
                for rc in rcs:
                    self._resource_list[rc] = rcs[rc]
            except Exception as e:
                print "Failed"
                logging.exception("{}:{}".format(str(e.__class__), str(e)))
                raise BundleException("Session.__init__: parsing resource config file Failed!")

            # push resource list to db
            self._dbs.add_resource_list(resource_list=self._resource_list)
            print "Success"

            print "Step (3 of 4): start bundle agents                  ..."
            # self._controller = BundleAgentController(session=self,
            #         db_connection=self._dbs)
            # add each resource to bundle_agent_controller
            for rc in self._resource_list:
                print "adding {}".format(rc)
                # self._controller.add_cluster(self._resource_list[rc])
                if self.add_agent(self._resource_list[rc]):
                    print "{} added".format(rc)
                else:
                    print "{} not added".format(rc)
            print "Step (3 of 4): start bundle agents                  ...",
            print "Success"

            print "Step (4 of 4): enter service mode                   ...",
            print self._database_url
            print self._database_name
            print self._uid
            print "Success"

        else:
            # TODO reconnect to existing session
            return

    def close(self, cleanup=True, terminate=True, delete=None):
        print "Closing session ..."
        for rid in self._agent_list.keys():
            self.remove_agent(resource_id=rid)

    @property
    def service_address(self):
        return "{}/{}.{}".format(self._database_url, self._databasename,
                                 self._uid)

    def load_resource_config_file(self, config_file):
        """Load all resource logins from one config file to a dict.
        """
        # TODO json, one org per file
        _cred = {}
        _valid_p     = ['cluster_type', 'login_server', 'username',
                        'password', 'key_file']
        _mandatory_p = ['cluster_type', 'login_server', 'username']
        _file = open(config_file, 'r')
        for _l in _file:
            _l = _l.strip()
            if _l and not _l.startswith('#'):
                _c = {}
                for _p in _l.split():
                    _k, _v = _p.split('=')
                    _k = _k.strip()
                    if _k not in _valid_p:
                        logging.error("Invalid key '{}': skip '{}'".format(_k, _l))
                        continue
                    _c[_k.strip()] = _v.strip()
                _missing_key = False
                for _k in _mandatory_p:
                    if _k not in _c:
                        logging.error("Missing mandatory key '{}': {}".format(_k, _l))
                        _missing_key = True
                        break
                if not _missing_key:
                    if _c['cluster_type'] not in BundleAgent.supported_types:
                        logging.error("Unsupported resource type '{}': {}".format(
                                _c['cluster_type'], _l))
                        continue
                    _cred[_c['login_server']] = _c
                else:
                    logging.error("Skip '{}'".format(_l))
        return _cred

    def add_resource(self, rc):
        """Add a new resource to the current session
        """
        pass

    def add_agent(self, rc):
        """Create a new BundleAgent instance
        """
        if  rc['login_server'] in self._agent_list:
            print "BundleAgent for {} already exists, try remove first".format(rc['login_server'])
            return None

        try:
            self._agent_list[rc['login_server']] = BundleAgent.create(
                    resource_config=rc, dbs=self._dbs)
            return self._agent_list[rc['login_server']]
        except Exception as e:
            print "Failed to creat new BundleAgent for {}:\n{}\n{}".format(
                    rc['login_server'], str(e.__class__), str(e))
            return None

    def remove_agent(self, resource_id):
        """Stop/Delete BundleAgent instance
        """
        if  resource_id not in self._agent_list:
            print "BundleAgent {} doesn't exists".format(resource_id)
            return
        self._agent_list[resource_id].close()
        del self._agent_list[resource_id]

