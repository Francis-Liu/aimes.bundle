#!/usr/bin/env python

import sys

class BundleManagerIntf(object):
    def __init__(self):
        try:
            import Pyro4
        except ImportError:
            print "Bundle Manager depends on Pyro4 package installation"
            sys.exit(1)
    	self.remote_bm = Pyro4.Proxy("PYRONAME:BundleManager")

    def add_cluster(self, cluster_credential, db_dir):
        return self.remote_bm.add_cluster(cluster_credential, db_dir)

    def remove_cluster(self, cluster_id):
        return self.remote_bm.remove_cluster(cluster_id)

    def get_cluster_list(self):
        return self.remote_bm.get_cluster_list()

    def get_cluster_configuration(self, cluster_id):
        return self.remote_bm.get_cluster_configuration(cluster_id)

    def get_cluster_workload(self, cluster_id):
        return self.remote_bm.get_cluster_workload(cluster_id)

    def get_cluster_bandwidths(self, cluster_id):
        return self.remote_bm.get_cluster_bandwidths(cluster_id)

    def set_verbosity(self, verbose=0):
        return self.remote_bm.set_verbosity(verbose)

    def __del__(self):
        pass

