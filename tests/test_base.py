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
import pytest
from btrdbextras import __version__, Connection


##########################################################################
## Initialization Tests
##########################################################################

class TestPackage(object):

    def test_version(self):
        """
        Assert that the test version smells valid.
        """
        assert bool(re.match(r"^v\d+\.\d+\.\d+$", __version__))

class TestConnection(object):

    def test_properties(self):
        """
        Connection contains apikey and endpoint
        """
        endpoint = "localhost:4411"
        apikey = "12345"
        conn = Connection(endpoint, apikey)
        assert conn.endpoint == endpoint
        assert conn.apikey == apikey

    def test_required_args(self):
        """
        Connection requires args
        """
        endpoint = "localhost:4411"
        apikey = "12345"

        with pytest.raises(ValueError):
            Connection(endpoint, None)

        with pytest.raises(ValueError):
            Connection(endpoint, "")

        with pytest.raises(ValueError):
            Connection(None, apikey)

        with pytest.raises(ValueError):
            Connection("", apikey)

        with pytest.raises(ValueError):
            Connection()

    def test_invalid_args(self):
        """
        Connection arg validation
        """
        apikey = "12345"
        with pytest.raises(ValueError):
            Connection("localhost", apikey)
