# -*- coding: utf-8 -*-
import os
from UserDict import UserDict

import saga
import radical.utils as ru

from aimes.bundle import BundleException


class ResourceBundle(object):
    def __init__(self, dbs):
        self._dbs = dbs
        self.resource_list = []
        # we have a dictionary of Resources instances, indexed by resource name
        self.resources = {}
        self.refresh_data()

    @staticmethod
    def create(dbs, bundle_description=None, bundle_manager_id=None):
        bundle = ResourceBundle(dbs)
        return bundle

    def refresh_data(self):
        self.resources = {}
        self.resource_list = self._dbs.get_resource_list()
        for resource_name in self.resource_list:

            config   = self._dbs.get_resource_config(resource_name)
            workload = self._dbs.get_resource_workload(resource_name)

            self.resources[resource_name] = Resource(resource_name, config, workload)


class Resource(UserDict):
    def __init__(self, name, config, workload):
        UserDict.__init__(self)
        self.name           = name
        self.num_nodes      = config['num_nodes']
        self.category       = config['category']
        self.update_time    = workload['timestamp']
        self["name"]        = self.name
        self['num_nodes']   = self.num_nodes
        self['category']    = self.category
        self["update_time"] = self.update_time
        if self.category == "hpc":
            # we have a list of Queue instances, to inspect queue information,
            # indexed by queue name
            self.queues = dict()
            for queue_name in config['queue_info']:
                self.queues[queue_name] = Queue(self.name, queue_name,
                                                config['queue_info'][queue_name],
                                                workload[queue_name])
            self['queues'] = self.queues
        elif self.category == "grid":
            self.num_queueing_jobs    = workload['num_queueing_jobs']
            self.busy_jobslots        = workload['busy_jobslots']
            self.idle_jobslots        = workload['idle_jobslots']
            self["num_queueing_jobs"] = self.num_queueing_jobs
            self["busy_jobslots"]     = self.busy_jobslots
            self["idle_jobslots"]     = self.idle_jobslots
            self.sites  = dict()
            for site_name in config['site_info']:
                self.sites[site_name] = Site(self.name, site_name,
                                             config['site_info'][site_name],
                                             workload[site_name])
            self['sites'] = self.sites


class Queue(object):
    def __init__(self, resource_name, name, config, workload):

        self.name              = name
        self.resource_name     = resource_name
        self.max_walltime      = config['max_walltime']
        self.num_procs_limit   = config['num_procs_limit']
        self.alive_nodes       = workload['alive_nodes']
        self.alive_procs       = workload['alive_procs']
        self.busy_nodes        = workload['busy_nodes']
        self.busy_procs        = workload['busy_procs']
        self.free_nodes        = workload['free_nodes']
        self.free_procs        = workload['free_procs']
        self.num_queueing_jobs = workload['num_queueing_jobs']
        self.num_running_jobs  = workload['num_running_jobs']

    def as_dict(self):
        object_dict = {
            "name"              : self.name,
            "resource_name"     : self.resource_name,
            "max_walltime"      : self.max_walltime,
            "num_procs_limit"   : self.num_procs_limit,
            "alive_nodes"       : self.alive_nodes,
            "alive_procs"       : self.alive_procs,
            "busy_nodes"        : self.busy_nodes,
            "busy_procs"        : self.busy_procs,
            "free_nodes"        : self.free_nodes,
            "free_procs"        : self.free_procs,
            "num_queueing_jobs" : self.num_queueing_jobs,
            "num_running_jobs"  : self.num_running_jobs,
        }
        return object_dict

    def __str__(self):
        return str(self.as_dict())


class Site(object):
    def __init__(self, resource_name, name, config, workload):

        self.name              = name
        self.resource_name     = resource_name
        self.num_nodes         = config['num_nodes']
        self.node_list         = config['node_list']
        self.busy_jobslots     = workload['busy_jobslots']
        self.idle_jobslots     = workload['idle_jobslots']

    def as_dict(self):
        object_dict = {
            "name"              : self.name,
            "resource_name"     : self.resource_name,
            "num_nodes"         : self.num_nodes,
            "node_list"         : self.node_list,
            "busy_jobslots"     : self.busy_jobslots,
            "idle_jobslots"     : self.idle_jobslots,
        }
        return object_dict

    def __str__(self):
        return str(self.as_dict())
