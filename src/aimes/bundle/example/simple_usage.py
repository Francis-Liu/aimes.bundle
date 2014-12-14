import json
import sys
from bundle import BundleManager

success = True

print "******    Step 01: create a BundleManager instance    ******"
bm = BundleManager()
if bm:
    print "SUCCEED!"
else:
    print "FAILED!"
    sys.exit(1)

print "******    Step 02: check BundleManager's active resource list    ******"
l = bm.get_cluster_list()
if l:
    print l
    print "SUCCEED!"
else:
    print "FAILED!"
    sys.exit(1)

print "******    Step 03: show each resource cluster's configuration    ******"
for c in l:
    print "Cluster {}:".format(c)
    cc = bm.get_cluster_configuration(c)
    if cc:
        print json.dumps(cc, sort_keys=True, indent=4)
    else:
        success = False
if success:
    print "SUCCEED!"
else:
    print "FAILED!"
    sys.exit(1)

print "******    Step 04: show each resource cluster's workload    ******"
for c in l:
    print "Cluster {}:".format(c)
    cc = bm.get_cluster_workload(c)
    if cc:
        print json.dumps(cc, sort_keys=True, indent=4)
    else:
        success = False
if success:
    print "SUCCEED!"
else:
    print "FAILED!"
    sys.exit(1)

print "******    Step 05: show bandwidth to destination clusters    ******"
for c in l:
    print "Cluster {}:".format(c)
    bw = bm.get_cluster_bandwidths(c)
    for tgt in bw :
        print "  %20s: 5.2f MB/s in  /  5.2f MB/s out" % (tgt, bw[tgt]['in'], bw[tgt]['out'])

if success:
    print "SUCCEED!"
else:
    print "FAILED!"
    sys.exit(1)

