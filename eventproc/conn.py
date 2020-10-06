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

INTERNAL_PORT="7598"


def connect(endpoint=os.environ.get("BTRDB_ENDPOINTS"), apikey=os.environ.get("BTRDB_APIKEY")):
    addrport = endpoint.split(":", 2)
    chan_ops = [] #[('grpc.default_compression_algorithm', CompressionAlgorithm.gzip)]

    if len(addrport) != 2:
        raise ValueError("expecting address:port")

    if addrport[1] == "4411":

        if apikey is None:
            channel = grpc.secure_channel(
                endpoint,
                options=chan_ops
            )
        else:
            channel = grpc.secure_channel(
                endpoint,
                grpc.composite_channel_credentials(
                    grpc.access_token_call_credentials(apikey)
                ),
                options=chan_ops
            )
    else:
        if apikey is not None and addrport[1] != INTERNAL_PORT:
            raise ValueError("cannot use an API key with an insecure (port 4410) BTrDB API. Try port 4411")
        channel = grpc.insecure_channel(endpoint, chan_ops)

    return channel
