
__author__    = "Francis Liu"
__copyright__ = "Copyright 2013-2015, The AIMES Project"
__license__   = "MIT"

import os
import sys
import aimes.bundle

def main():
    try:
        s = aimes.bundle.Session()
        bm1 = aimes.bundle.BundleManager(session=s)

        # List all RB ids
        #
        # Since no user-defined RBs were created, this call will
        # return the default RB id "all", which includes all
        # resources provided by the resource config file.
        rbList1 = bm1.list_resource_bundles()
        assert len(rbList1) == 1

        # list resource details of the default RB
        for rb_id in rbList1:
            rb = bm1.get_resource_bundle(rb_id)
            for resource in rb.resources:
                print resource
                for queue in rb.resources[resource].queues:
                    print queue
                    print rb.resources[resource].queues[queue]

        return 0
    except aimes.bundle.BundleException, ex:
        return -1

if __name__ == "__main__":
        sys.exit(main())
