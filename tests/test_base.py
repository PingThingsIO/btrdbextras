# tests.test_base
# Testing package for the btrdbextras library.
#
# Author:   PingThings
# Created:  Tue Oct 20 14:23:25 2020 -0500
#
# For license information, see LICENSE.txt
# ID: test_base.py [] allen@pingthings.io $

"""
Testing package for the btrdb database library.
"""

##########################################################################
## Imports
##########################################################################

import re
from btrdbextras import __version__


##########################################################################
## Initialization Tests
##########################################################################

class TestPackage(object):

    def test_version(self):
        """
        Assert that the test version smells valid.
        """
        assert bool(re.match(r"^v\d+\.\d+\.\d+$", __version__))