#!/usr/bin/env python

__author__    = "TODO"
__copyright__ = "Copyright 2013, TODO"
__license__   = "MIT"


from exceptions import *
from session import Session
from bundle_manager import BundleManager

# ------------------------------------------------------------------------------
import os

_mod_root = os.path.dirname (__file__)

version        = open (_mod_root + "/VERSION",     "r").readline ().strip ()
version_detail = open (_mod_root + "/VERSION.git", "r").readline ().strip ()
# ------------------------------------------------------------------------------

