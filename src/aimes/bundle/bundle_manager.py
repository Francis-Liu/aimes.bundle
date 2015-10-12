# -*- coding: utf-8 -*-

import os
import sys
import time
import Pyro4
import socket
import logging

import radical.utils  as ru

from aimes.bundle     import BundleException
from aimes.bundle.db  import DBException
from aimes.bundle.db  import Session as dbSession
import resource_bundle as ResourceBundle


class BundleManager(object):
    def __init__(self, session_id, database_url=None,
                 database_name="AIMES_bundle", uid=None):
        self._sid           = session_id
        self._database_url  = database_url
        self._database_name = database_name
        self._uid           = uid
        self._dbs           = None
        self._dbs_metadata  = None
        # self._dbs_connection_info = None
        self._ip_addr       = socket.gethostbyname(socket.getfqdn())
        self._resource_cfgs = None # will be a dict
        # { rb_id : rb_obj }
        self._resource_bundles = None
        self._pyro_uri      = ''
        self._bundled       = None

        # Step1: connect to db
        try:
            # self._dbs, self._dbs_metadata, self._dbs_connection_info = \
            self._dbs, self._dbs_metadata = \
                    dbSession.reconnect(sid     = self._sid,
                                        db_url  = self._database_url,
                                        db_name = self._database_name)
            if self._uid == None:
                self._uid = ru.generate_id('bundle_manager', mode=ru.ID_PRIVATE)

            self._dbs.register_bundle_manager(bm_id=self._uid, ip_addr=self._ip_addr)
            self._resource_cfgs = self._dbs.get_resource_cfgs()
        except Exception as e:
            logging.exception("{}:{}".format(str(e.__class__), str(e)))
            raise BundleException("BundleManager.__init__: db connection Failed!")

        self._pyro_uri      = self._dbs.get_pyro_uri()
        self._bundled       = Pyro4.Proxy(self._pyro_uri)
        print self._bundled.version
        self._resource_bundles = [ResourceBundle.ResourceBundle.create(self._dbs)]

    @property
    def uid(self):
        return self._uid

    @property
    def resource_cfgs(self):
        return self._resource_cfgs

    @property
    def resource_bundles(self):
        return self._resource_bundles

    def add_network_monitor(self, res_name, end_point, frequency=3600):
        return self._bundled.add_network_monitor(agent_id=res_name, tgt=end_point, frequency=frequency)
