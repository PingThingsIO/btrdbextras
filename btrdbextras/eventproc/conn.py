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
import grpc

PATH_PREFIX="/eventproc"


def connect(endpoint=os.environ.get("BTRDB_ENDPOINTS"), apikey=os.environ.get("BTRDB_API_KEY")):
    addrport = endpoint.split(":", 2)
    endpoint += PATH_PREFIX

    if len(addrport) != 2:
        raise ValueError("expecting address:port")

    if apikey is None or apikey == "":
        raise ValueError("must supply an API key")

    return grpc.secure_channel(
        endpoint,
        grpc.composite_channel_credentials(
            grpc.ssl_channel_credentials(None),
            grpc.access_token_call_credentials(apikey)
        )
    )
