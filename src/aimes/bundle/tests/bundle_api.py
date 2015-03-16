#!/usr/bin/env python

# pylint: disable-msg=C0103

"""Test the bundles API.
"""

__author__ = "Matteo Turilli, Andre Merzky"
__copyright__ = "Copyright 2014, RADICAL"
__license__ = "MIT"

import os
import radical.utils as ru
import aimes.bundle

# Set environment variables.
ORIGIN = os.getenv("BUNDLE_ORIGIN")
BUNDLE_DBURL = os.getenv("BUNDLE_DBURL")

# Create a reporter for the test. Takes care of colors and font attributes.
report = ru.Reporter(title='Bundle API test')

bundle = aimes.bundle.Bundle(query_mode=aimes.bundle.DB_QUERY,
                             mongodb_url=BUNDLE_DBURL,
                             origin=ORIGIN)

# Collect information about the resources to plan the execution strategy.
bandwidth_in = dict()
bandwidth_out = dict()

# Get network bandwidth for each resource.
for resource_name in bundle.resources:
    resource = bundle.resources[resource_name]
    bandwidth_in[resource.name] = resource.get_bandwidth(ORIGIN, 'in')
    bandwidth_out[resource.name] = resource.get_bandwidth(ORIGIN, 'out')

# Report back to the demo about the available resource bundle.
report.info("Target Resources")
print "IDs: %s" % \
    [bundle.resources[resource].name for resource in bundle.resources]

# Print all the information available via the bundle API.
for resource_name in bundle.resources:
    resource = bundle.resources[resource_name]

    report.info("resource.name     : %s" % resource.name)
    print "resource.num_nodes: %s" % resource.num_nodes
    print "resource.container: %s" % resource.container
    print "resource.get_bandwidth(IP, 'in') : %s" % \
        resource.get_bandwidth(ORIGIN, 'in')

    print "resource.get_bandwidth(IP, 'out'): %s" % \
        resource.get_bandwidth(ORIGIN, 'out')

    print "resource.queues   : %s" % resource.queues.keys()

    for queue_name in resource.queues:
        queue = resource.queues[queue_name]
        print
        print "  queue.name             : %s" % queue.name
        print "  queue.resource_name    : %s" % queue.resource_name
        print "  queue.max_walltime     : %s" % queue.max_walltime
        print "  queue.num_procs_limit  : %s" % queue.num_procs_limit
        print "  queue.alive_nodes      : %s" % queue.alive_nodes
        print "  queue.alive_procs      : %s" % queue.alive_procs
        print "  queue.busy_nodes       : %s" % queue.busy_nodes
        print "  queue.busy_procs       : %s" % queue.busy_procs
        print "  queue.free_nodes       : %s" % queue.free_nodes
        print "  queue.free_procs       : %s" % queue.free_procs
        print "  queue.num_queueing_jobs: %s" % queue.num_queueing_jobs
        print "  queue.num_running_jobs : %s" % queue.num_running_jobs
