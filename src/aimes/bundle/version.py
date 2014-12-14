#!/usr/bin/env python
# encoding: utf-8

__author__    = "Francis Liu"
__copyright__ = "Copyright 2013, AIMES project"
__license__   = "MIT"

import os

# figure out the current version
# version is defined in VERSION
version = "latest"

try:
    cwd = os.path.dirname(os.path.abspath(__file__))
    fn = os.path.join(cwd, 'VERSION')
    version = open(fn).read().strip()
except IOError:
    from subprocess import Popen, PIPE, STDOUT
    import re

    VERSION_MATCH = re.compile(r'\d+\.\d+\.\d+(\w|-)*')

    try:
        p = Popen(['git', 'describe', '--tags', '--always'],
                  stdout=PIPE, stderr=STDOUT)
        out = p.communicate()[0]

        if (not p.returncode) and out:
            v = VERSION_MATCH.search(out)
            if v:
                version = v.group()
    except OSError:
        pass
