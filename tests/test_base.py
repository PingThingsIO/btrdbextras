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

from btrdbextras import __version__

##########################################################################
## Test Constants
##########################################################################

EXPECTED_VERSION = "5.11.0"


##########################################################################
## Initialization Tests
##########################################################################

class TestPackage(object):

    def test_version(self):
        """
        Assert that the test version matches the library version.
        """
        assert __version__ == EXPECTED_VERSION