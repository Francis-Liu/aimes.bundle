
import os
import sys
import time
import logging
import radical.utils as ru

from aimes.bundle.resource_bundle import ResourceBundle

def ip2id (ip) :
    return ip.replace ('.', '_')

class BundleManager(ru.Daemon):

    # --------------------------------------------------------------------------
    #
    def __init__(self, session, _reconnect=False):

        # initialize the daemon class
        super (BundleManager, self).__init__ ()

        self._session = session
        self._controller = None
        self._uid = None

        if _reconnect:
            return

        # create the default all-resources RB
        # TODO
        bundle_description = {}
        bundle = ResourceBundle.create(
                bundle_description=bundle_description,
                bundle_manager_id=self._uid)

        # TODO
        # bundle_uid = self._session.register_new_resource_bundle(
        #         bundle=bundle)

        self.resource_bundle_list = {"default" : bundle}

        # self.bandwidths = dict()
        # bw_cfg = '%s/etc/aimes_bundle_bandwidth.cfg' % sys.prefix
        # 
        # with open (bw_cfg, 'r') as f_read :
        #     for line in f_read :

        #         line = line.strip ()

        #         if  line and not line.startswith ('#') :

        #             src, dst, bw = line.split (',')

        #             if  not src in self.bandwidths :
        #                 self.bandwidths[src]  = dict()

        #             self.bandwidths[src][dst] = bw


    # --------------------------------------------------------------------------
    #
    # List all RB ids
    def list_resource_bundles(self):
        return self.resource_bundle_list.keys()


    # --------------------------------------------------------------------------
    #
    # Key method
    # Return RB id
    def create_resource_bundle(self, resource_bundle_configuration):
        return


    # --------------------------------------------------------------------------
    #
    def get_resource_bundle(self, rb_id):
        return self.resource_bundle_list[rb_id]


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


