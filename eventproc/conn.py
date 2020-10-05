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
import re
import json
import uuid as uuidlib

import grpc
from grpc._cython.cygrpc import CompressionAlgorithm


def connect(endpoint=os.environ.get("BTRDB_ENDPOINTS"), apikey=os.environ.get("BTRDB_APIKEY")):
    addrport = endpoint.split(":", 2)
    chan_ops = [('grpc.default_compression_algorithm', CompressionAlgorithm.gzip)]

    if len(addrport) != 2:
        raise ValueError("expecting address:port")

    if addrport[1] == "4411":
        # grpc bundles its own CA certs which will work for all normal SSL
        # certificates but will fail for custom CA certs. Allow the user
        # to specify a CA bundle via env var to overcome this
        contents = None

        if apikey is None:
            channel = grpc.secure_channel(
                endpoint,
                grpc.ssl_channel_credentials(contents),
                options=chan_ops
            )
        else:
            channel = grpc.secure_channel(
                endpoint,
                grpc.composite_channel_credentials(
                    grpc.ssl_channel_credentials(contents),
                    grpc.access_token_call_credentials(apikey)
                ),
                options=chan_ops
            )
    else:
        if apikey is not None:
            raise ValueError("cannot use an API key with an insecure (port 4410) BTrDB API. Try port 4411")
        channel = grpc.insecure_channel(endpoint, chan_ops)

    return channel
