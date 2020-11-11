# eventproc.conn
# Connection related objects
#
# Author:   PingThings
# Created:
#
# For license information, see LICENSE.txt
# ID: conn.py [] allen@pingthings.io $

"""
Connection related objects
"""

##########################################################################
## Imports
##########################################################################

import os

##########################################################################
## Classes
##########################################################################

class Connection():

    def __init__(self, endpoint=os.environ.get("BTRDB_ENDPOINTS"), apikey=os.environ.get("BTRDB_API_KEY")):
        if endpoint is None or endpoint.strip() == "":
            raise ValueError("invalid endpoint or BTRDB_ENDPOINTS env variable not set")

        if apikey is None or apikey.strip() == "":
            raise ValueError("invalid api key or BTRDB_API_KEY env variable not set")

        if ":" not in endpoint:
            raise ValueError("invalid endpoint")

        self._endpoint = endpoint
        self._apikey = apikey

    @property
    def apikey(self):
        return self._apikey

    @property
    def endpoint(self):
        return self._endpoint
