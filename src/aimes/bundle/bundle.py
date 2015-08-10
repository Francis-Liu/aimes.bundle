
__author__    = "Francis Liu, Matteo Turilli, Andre Merzky"
__copyright__ = "Copyright 2013-2014, The AIMES Project"
__license__   = "MIT"


import radical.utils as ru

import pprint
import impl

# -----------------------------------------------------------------------------
#
# The Bundle class has two different query modes: 
#
# DIRECT_QUERY: create a bundle manager, which creates Bundle agents, which 
#               query the systems
#
# DB_QUERY    : connect to a mongodb service and fetch data from there, where
#               a bundle daemon deposited them
DIRECT_QUERY = 'direct_query'
DB_QUERY     = 'db_query'

DEFAULT_MONGODB_URL = 'mongodb://ec2-184-72-89-141.compute-1.amazonaws.com:27017/bundle_v0_1/'
DEFAULT_MONGODB_URL = 'mongodb://localhost:27017/bundle_v0_1/'
DEFAULT_MONGODB_URL = 'mongodb://54.221.194.147:24242/AIMES_bundle_fengl_osg/"

# -----------------------------------------------------------------------------
#
class Bundle(object):
    """
    The main class, Bundle, accepts a config file and an 'origin' resource name.
    The latter is used for bandwidth information queries, which are always
    relative to the given origin.  The class uses the bundle implementation to
    create a Resource instance for each given resource.
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, config_file=None, origin=None, 
                 query_mode=DB_QUERY, 
                 mongodb_url=DEFAULT_MONGODB_URL):

        self.config_file = config_file
        self.origin      = origin
        self.mongodb_url = mongodb_url

        if  query_mode == DIRECT_QUERY :
            self.query_direct ()

        elif query_mode == DB_QUERY :
            self.query_db ()

        else :
            raise ValueError ("no such query mode '%s'" % query_mode)


    # --------------------------------------------------------------------------
    #
    def query_direct (self) :

        self.bm = impl.BundleManager()
        self.bm.load_cluster_credentials(self.config_file)

        self.resource = list()
        self._priv    = self.bm.get_data(self.origin)

      # for cluster in self._priv['cluster_list']:
      #     print "===> %s" % cluster
      #     print
      #     print "--   config"
      #     pprint.pprint(self._priv['cluster_config'][cluster])
      #     print
      #     print "--   workload"
      #     pprint.pprint(self._priv['cluster_workload'][cluster])
      #     print
      #     print "--   bandwidths"
      #     pprint.pprint(self._priv['cluster_bandwidth'][cluster])
      #     print
      #     print

        # we have a dictionary of Resources instances, indexed by resource name
        self.resources = dict()
        for resource_name in self._priv['cluster_list']:

            config     = self._priv['cluster_config'   ][resource_name]
            workload   = self._priv['cluster_workload' ][resource_name]
            bandwidths = self._priv['cluster_bandwidth'][resource_name]

            # import pprint
            # pprint.pprint(bandwidths)

            self.resources[resource_name] = Resource(resource_name, config, workload, bandwidths)


        # and a list of Queue instances, for all queues of all resources
        self.queues = list()
        for resource in self.resources:
            self.queues += self.resources[resource].queues.values()


    # --------------------------------------------------------------------------
    #
    def query_db (self) :

        mongo, db, dbname, cname, pname = ru.mongodb_connect (self.mongodb_url)

        self._priv = dict()
        self._priv['cluster_list']      = list()
        self._priv['cluster_config']    = dict()
        self._priv['cluster_workload']  = dict()
        self._priv['cluster_bandwidth'] = dict()


        for doc in list(db['config'].find ()):
            self._priv['cluster_list'].append (doc['_id'])
            self._priv['cluster_config'][doc['_id']] = doc

        for doc in list(db['workload'].find ()):
            self._priv['cluster_workload'][doc['_id']] = doc

        for doc in list(db['bandwidth'].find ()):
            self._priv['cluster_bandwidth'][doc['_id']] = doc


        # we have a dictionary of Resources instances, indexed by resource name
        self.resources = dict()
        for resource_name in self._priv['cluster_list']:

            config     = self._priv['cluster_config'   ].get (resource_name, dict())
            workload   = self._priv['cluster_workload' ].get (resource_name, dict())
            bandwidths = self._priv['cluster_bandwidth'].get (resource_name, dict())

            # import pprint
            # pprint.pprint(bandwidths)

            self.resources[resource_name] = Resource(resource_name, config, workload, bandwidths)


        # and a list of Queue instances, for all queues of all resources
        self.queues = list()
        for resource in self.resources:
            self.queues += self.resources[resource].queues.values()

        
# -----------------------------------------------------------------------------
#
class Resource(object):
    """
    This class represents a set of information on a resource.
    Specifically, the class also has a list of Queue instances, which
    represent information about the resource's batch queues.
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, config, workload, bandwidths):

        self.name       = name
        self.num_nodes  = config['num_nodes']
        self.container  = 'job'   # FIXME: what are the other options?
        self.bandwidths = bandwidths

        # we have a list of Queue instances, to inspect queue information,
        # indexed by queue name
        self.queues = dict()
        for queue_name in config['queue_info']:
            self.queues[queue_name] = Queue(self.name, queue_name,
                                            config['queue_info'][queue_name],
                                            workload[queue_name])


    # --------------------------------------------------------------------------
    #
    def get_bandwidth(self, tgt, mode):

        if  tgt in self.bandwidths:
            return self.bandwidths[tgt][mode]

        return 0.0


# -----------------------------------------------------------------------------
#
class Queue(object):
    """
    This class represents a set of information on a batch queue of a
    specific resource.
    """

    # --------------------------------------------------------------------------
    #
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

# -----------------------------------------------------------------------------

