
__author__    = "Francis Liu"
__copyright__ = "Copyright 2013-2015, The AIMES Project"
__license__   = "MIT"

import sys
import aimes.bundle

def main():
    try:
        bm1 = aimes.bundle.BundleManager(session=s, resource_configurations=RESCONF)
        bm1.list_bundles()
        b1 = bm1.get_bundles(bundle_ids='osg.uct2')
        b1.list_resources()
        # Create resource bundle on OSG
        # Show them what can Bundle-OSG do:
        #   Show configuration?
        #       clusters ['UCT2', ...]
        #           groups
        #   Show capacity, size
        #   Show workload
        return 0
    except aimes.bundle.BundleException, ex:
        return -1

if __name__ == "__main__":
        sys.exit(main())
