
import os
import threading

import aimes.bundle.agent.bundle_agent

# ----------------------------------------------------------------------------
#
class BundleAgentController(threading.Thread):

    # ------------------------------------------------------------------------
    #
    def __init__(self, session, db_connection):
        self._session = session
        self._db = db_connection
        self.cluster_list = {}
        self.cluster_credentials = []
        threading.Thread.__init__(self)
        return


    # ------------------------------------------------------------------------
    #
    def add_cluster(self, cluster_credential):
        if cluster_credential['hostname'] in self.cluster_list:
            #logging.debug("Cluster hostname '{}' already added, will skip it.".format(cluster_credential['hostname']))
            return cluster_credential['hostname']
        cluster = aimes.bundle.agent.bundle_agent.create(cluster_credential)
        if cluster:
            self.cluster_list[cluster_credential['hostname']] = cluster
            self.cluster_credentials.append(dict(cluster_credential))
            return cluster_credential['hostname']
        return None


    # ------------------------------------------------------------------------
    #
    def remove_cluster(self, cluster_id):
        if cluster_id in self.cluster_list:
            self.cluster_list[cluster_id].close()
            self.cluster_list.pop(cluster_id, None)
        else:
            #logging.debug("Cluster hostname '{}' doesn't exist".format(cluster_id))
            pass


    # ------------------------------------------------------------------------
    #
    def get_cluster_list(self):
        return self.cluster_list.keys()


    # ------------------------------------------------------------------------
    #
    def get_cluster_configuration(self, cluster_id):
        if cluster_id in self.cluster_list:
            return self.cluster_list[cluster_id].get_configuration()
        else:
            #logging.debug("Cluster hostname '{}' doesn't exist".format(cluster_id))
            return None


    # ------------------------------------------------------------------------
    #
    def get_cluster_workload(self, cluster_id):
        if cluster_id in self.cluster_list:
            return self.cluster_list[cluster_id].get_workload()
        else:
            #logging.debug("Cluster hostname '{}' doesn't exist".format(cluster_id))
            return None


    # ------------------------------------------------------------------------
    #
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


    # ------------------------------------------------------------------------
    #
    @classmethod
    def uid_exists(cls, db_connection, bundle_manager_uid):
        return

    # ------------------------------------------------------------------------
    #
    @property
    def bundle_manager_uid(self):
        return

    # ------------------------------------------------------------------------
    #
    def list_bundle_agents(self):
        return self._db.list_bundle_uids(self._pm_id)


    # ------------------------------------------------------------------------
    #
    def get_compute_bundle_data(self, bundle_ids=None):
        return


    # ------------------------------------------------------------------------
    #
    def stop(self):
        return


    # ------------------------------------------------------------------------
    #
    def run(self):
        return


    # ------------------------------------------------------------------------
    #
    def kill_bundle_agent(self, bundle_ids=None):
        return

