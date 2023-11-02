# permalink
# Python function to create a PredictiveGrid plotter permalink
#
# Author:   PingThings
# Created:  Tues July 28 2020
#
# Copyright (C) 2020 PingThings LLC
# For license information, see LICENSE.txt
#
# ID: permalink.py [] michael.chestnut@pingthings.io $

"""
Create a PredictiveGrid plotter permalink
"""

##########################################################################
# Imports
##########################################################################

import base64
import json
import os

import btrdb
import requests
from btrdb.utils.timez import to_nanoseconds

##########################################################################
# Constants
##########################################################################

COLORS = ["#e41a1c", "#377eb8", "#05b100", "#984ea3", "#e27100", "#635716"]
BASE_URI = "internal.plot.dominion.predictivegrid.com"
OUTPUT_URI = "plot.dominion.predictivegrid.com"

##########################################################################
# Helpers
##########################################################################


class Base64Encoder(json.JSONEncoder):
    """
    Allows json serialization of a dictionary that already
    contains json serialized objects
    """

    # pylint: disable=method-hidden
    def default(self, o):
        if isinstance(o, bytes):
            return base64.b64encode(o).decode()
        return json.JSONEncoder.default(self, o)


def get_token(username, password, base_uri=None):
    """
    Generates bearer token to be used in graphql queries.
    """

    mutation = """
        mutation Login($username: String!, $password: String!) {
            Login(body: {username: $username, password: $password}) {
                username
                token
                __typename
            }
        }
    """
    variables = dict(username=username, password=password)

    data = dict(query=mutation, operationName="Login", variables=variables)

    headers = {"content-type": "application/json"}

    res = requests.post(
        f"https://{base_uri}/api/graphql", data=json.dumps(data), headers=headers
    )

    return res.json()["data"]["Login"]["token"]


def get_stream_meta(stream, color, legend):
    """
    Populates dictionary of metadata for a BTrDB stream
    """

    legend_name = {
        "collection": stream.collection,
        "description": stream.annotations()[0].get("description", legend),
    }.get(legend, stream.unit)

    return {
        "metadata": {
            "annotations": {k: v for k, v in stream.annotations()[0].items()},
            "collection": stream.collection,
            "property_version": stream._property_version,
            "tags": {k: v for k, v in stream.tags().items()},
            "uuid": str(stream.uuid),
        },
        "plot": {
            "uuid": str(stream.uuid),
            "unit": stream.unit,
            "color": color,
            "visibility": True,
            "legendName": legend_name,
        },
    }


def get_plotter_state(streams, start, end, pointwidth, legend):
    """
    Returns plotter state object as json str

    Parameters
    ----------
    streams: dict containing a btrdb.Stream and a color string
    unit: str: abbreviation of btrdb.Stream unit to be used in plotter
    start: int or datetime like object
    end: int or datetime like object
    pointwidth: a btrdb.utils.general.pointwidth object
    legend: str value to display in the plotter legend

    Returns
    ----------
    json representation of plotter state
    """

    # List all unique units
    units = set([stream["unit"] for stream in streams])

    # alternate right/left side for axes
    def pick_side():
        while True:
            yield "right"
            yield "left"

    side = pick_side()

    return json.dumps(
        dict(
            axes={
                unit: {
                    "unit": unit,
                    "streams": [
                        str(stream["stream"].uuid)
                        for stream in streams
                        if stream["stream"].unit == unit
                    ],
                    "side": next(side),
                }
                for unit in units
            },
            chart=dict(
                start=start,
                end=end,
                resolution=pointwidth,
                # Need to list all units out here
                axes={unit: [-5000, 2000] for unit in units},
                selection=[],
            ),
            legend=dict(visible=True),
            streams={
                str(str(stream["stream"].uuid)): get_stream_meta(
                    stream["stream"], stream["color"], legend
                )
                for stream in streams
            },
        )
    )


def get_data(db, uuids, start, end, pointwidth, legend):
    """
    Returns json object containing query and variables needed
    to post to graphql api
    """

    # Use default start/end times if none provided
    start = to_nanoseconds(start) or btrdb.MINIMUM_TIME
    end = to_nanoseconds(end) or btrdb.MAXIMUM_TIME

    # Instantiate streams from provided uuids
    if isinstance(uuids, str):
        streams = [
            {
                "stream": db.stream_from_uuid(uuids),
                "color": COLORS[0],
                "unit": db.stream_from_uuid(uuids).unit,
            }
        ]
    elif isinstance(uuids, list):
        streams = [
            {
                "stream": db.stream_from_uuid(uu),
                "color": COLORS[i % len(COLORS)],
                "unit": db.stream_from_uuid(uu).unit,
            }
            for i, uu in enumerate(uuids)
        ]
    else:
        raise Exception(f"Invalid type provided for uuids: {type(uuids)}")

    # Graphql mutation
    query = """
        mutation CreatePermalink($data: Json!, $bookmark: Json, $public: Json) {
            CreatePermalink(body: { data: $data, bookmark: $bookmark, public: $public }) {
              error {
                code
                msg
              }
              permalink {
                uuid
              }
            }
          }
    """

    # Generate json of plotter state
    pstate = get_plotter_state(streams, start, end, pointwidth, legend)

    # Fill variables into graphql mutation
    variables = dict(data=pstate)

    return json.dumps(
        dict(query=query, operationName="CreatePermalink", variables=variables),
        cls=Base64Encoder,
    )


##########################################################################
# Get Permalink Function
##########################################################################


def get_permalink(
    db,
    uuids,
    username,
    password,
    start=None,
    end=None,
    pointwidth=45,
    legend="collection",
    base_uri=None,
    output_uri=None,
):
    """
    Generates a permalink for specific stream(s) and time period in the plotter

    Parameters
    ----------
    db: a btrdb.conn.BTrDB object
    uuids: a str or list of str of uuids for BTrDB streams
    username: str: PredictiveGrid username
    password: str: PredictiveGrid password
    start: int or datetime like object, default: btrdb.MINIMUM_TIME
    end: int or datetime like object, default: btrdb.MAXIMUM_TIME
    pointwidth: a btrdb.utils.general.pointwidth object
    legend: str: value to display in the plotter legend. Choices are unit, description, or collection
    base_uri: str: URI for graphql where data is posted to create permalink
    output_uri: str: URI used to build final permalink

    Returns
    -------
    str
        A permalink that can be used to view a stream/streams in the PredictiveGrid plotter
    """
    base_uri = base_uri or BASE_URI
    output_uri = output_uri or OUTPUT_URI

    # Get bearer token
    token = get_token(username, password, base_uri=base_uri)

    headers = {
        "Authorization": f"Bearer {token}",
        "content-type": "application/json",
    }

    # json data to POST to graphql api
    data = get_data(db, uuids, start, end, pointwidth, legend)

    # POST to graphql api
    res = requests.post(f"https://{base_uri}/api/graphql", data=data, headers=headers)

    # Create permalink from response
    id = res.json()["data"]["CreatePermalink"]["permalink"]["id"]
    return f"https://{output_uri}/permalink/{id}"


if __name__ == "__main__":
    db = btrdb.connect()

    uuids = [
        str(stream.uuid)
        for stream in db.streams_in_collection("dfr/DFR!PMU-PEN1_594_dfr_add1_09825e11")
    ]

    start = "2019-09-01 00:00:00.00"
    end = "2020-10-01 00:00:00.00"

    # Requires btrdb credentials as env variables
    link = get_permalink(
        db,
        uuids,
        os.getenv("USERNAME"),
        os.getenv("PASSWORD"),
        start=start,
        end=end,
        legend="description",
    )
    print(link)
