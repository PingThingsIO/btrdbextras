# btrdbextras.eventproc
# Event processing related functions.
#
# Author:   PingThings
# Created:  Fri Dec 21 14:57:30 2018 -0500
#
# For license information, see LICENSE.txt
# ID: conn.py [] allen@pingthings.io $

"""
Event processing related functions.
"""

##########################################################################
## Imports
##########################################################################

import io
from collections import namedtuple

import dill
from btrdb.utils.timez import ns_to_datetime

from btrdbextras.eventproc.protobuff import api_pb2
from btrdbextras.eventproc.protobuff import api_pb2_grpc

import os
import json
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from datetime import datetime

__all__ = ['hooks', 'list_handlers', 'register', 'deregister', 'upload_file']

import grpc

PATH_PREFIX="/eventproc"

##########################################################################
## Helper Functions
##########################################################################

def connect(conn):
    parts = conn.endpoint.split(":", 2)
    endpoint = conn.endpoint + PATH_PREFIX
    apikey = conn.apikey

    if len(parts) != 2:
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

def check_s3_creds(s3client, bucket):
    try:
        buckets = s3client.list_buckets()["Buckets"]
    except NoCredentialsError as e:
        return False
    found = False
    for b in buckets:
        if b["Name"] == bucket:
            found = True
    return found

##########################################################################
## Helper Classes
##########################################################################

HandlerBase = namedtuple("HandlerBase", "id name hook version notify_on_success notify_on_failure tags created_at created_by updated_at updated_by")

class Handler(HandlerBase):
    """
    Class definition for an event handler object.  Inherits from HandlerBase
    to add methods to the namedtuple.
    """

    @classmethod
    def from_grpc(cls, h):
        return cls(
            h.id, h.name, h.hook, h.version, h.notify_on_success, h.notify_on_failure,
            h.tag, ns_to_datetime(h.created_at), h.created_by,
            ns_to_datetime(h.updated_at), h.updated_by
        )

class Service(object):
    """
    Helper class to integrate with GRPC generated code.
    """

    def __init__(self, channel):
        self.channel = channel
        self.stub = api_pb2_grpc.EventProcessingServiceStub(channel)

    def ListHooks(self):
        params = api_pb2.ListHooksRequest()
        return [r.name for r in self.stub.ListHooks(params).hooks]

    def ListHandlers(self, hook):
        params = api_pb2.ListHandlersRequest(hook=hook)
        response = self.stub.ListHandlers(params)

        for result in response.handlers:
            yield result

    def Register(self, name, hook, func, apikey, notify_on_success, notify_on_failure, dependencies, tags):
        # convert decorated function to bytes
        buff = io.BytesIO()
        dill.dump(func, buff)
        buff.seek(0)

        params = api_pb2.RegisterRequest(
            registration=api_pb2.Registration(
            name=name,
            hook=hook,
            blob=buff.read(),
            api_key=apikey,
            notify_on_success=notify_on_success,
            notify_on_failure=notify_on_failure,
            dependencies=dependencies,
            tags=tags,
            )
        )

        response = self.stub.Register(params)
        if hasattr(response, "handler"):
            return Handler.from_grpc(response.handler)

    def Deregister(self, handler_id):
        params = api_pb2.DeregisterRequest(id=handler_id)
        response = self.stub.Deregister(params)
        return response


##########################################################################
## Public Functions
##########################################################################

def hooks(conn):
    """
    List registered hooks.

    Parameters
    ----------
    conn: Connection
        btrdbextras Connection object containing a valid address and api key.
    """
    s = Service(connect(conn))
    return s.ListHooks()

def list_handlers(conn, hook=""):
    """
    List registered handlers.  An optional hook name is allowed to filter
    results.

    Parameters
    ----------
    conn: Connection
        btrdbextras Connection object containing a valid address and api key.
    hook: str
        Optional hook name to filter registered handlers.

    """
    s = Service(connect(conn))
    return [Handler.from_grpc(h) for h in s.ListHandlers(hook)]


def deregister(conn, handler_id):
    """
    Removes an existing event handler by ID.

    Parameters
    ----------
    conn: Connection
        btrdbextras Connection object containing a valid address and api key.
    handler_id: int
        ID of the event handler to remove.

    """
    s = Service(connect(conn))
    h = s.Deregister(handler_id)
    return h.id == handler_id


def register(conn, name, hook, notify_on_success, notify_on_failure, tags=None):
    """
    decorator to submit (register) an event handler function

    Parameters
    ----------
    conn: Connection
        btrdbextras Connection object containing a valid address and api key.
    name: str
        Friendly name of this event handler for display purposes.
    hook: str
        Name of the hook that this event handler responds to.
    notify_on_success: str
        Email address of user to notify when event handler completes successfully.
    notify_on_failure: str
        Email address of user to notify when event handler does not complete
        successfully.
    tags: list of str
        Filtering tags that users can choose when identifying handlers to
        execute. An empty list will match all tags.

    """
    # placeholder for future dependency management feature
    dependencies = ""

    # inner will actually receive the decorated func but we still have access
    # to the args & kwargs due to closure/scope.
    def inner(func):

        # call grpc service to register event handler
        s = Service(connect(conn))
        _ = s.Register(name, hook, func, conn.apikey, notify_on_success, notify_on_failure, dependencies, tags)

        # return original func back to user
        return func

    return inner

def upload_file(file, file_name):
    """
    Uploads the file to S3, calling it file_name.

    Parameters
    ----------
    file: string or a readable file-like object
        Path to the file, or a readable file-like object.
    file_name: string
        Name that the file will be called in s3.
    return: False if the inputs were malformed or if the upload failed, else True. If the function was run outside of an eventproc handler, it will simply check the inputs, return True if they are well-formed, and not upload anything.
    """
    bucket = os.getenv("BUCKET")
    s3client = boto3.client('s3')

    openfile = False
    f = file
    
    # check the inputs
    if not isinstance(file_name, str):
        # throw error
        return False, None
    if isinstance(file, str):
        if not os.path.exists(file):
            # throw error
            return False, None
        openfile = True
    else:
        if not hasattr(file, "read"):
            # throw error
            return False, None

    # check the s3 connection
    if not check_s3_creds(s3client, bucket):
        print("WARNING: upload_file is running in an execution context without the appropriate AWS credentials and will not upload to S3, but still return True if the inputs are well-formed.")
        return True, None
    
    # get job metadata
    mdstr = os.getenv("JOB_MD")
    if mdstr != None:
        md = json.loads(mdstr)
    else:
        md = json.loads("{}")

    if openfile:
        f = open(file, "rb")
        
    # do the upload
    key = "uploads/"+datetime.now().strftime("%Y_%m_%d-%H_%M_%S")+"/"+file_name
    try:
        response = s3client.put_object(Body=f, Bucket=bucket, Key=key, Metadata=md)
    except ClientError as e:
        print(e)
        if openfile:
            f.close()
        return False, None
    if openfile:
        f.close()
    
    if result['ResponseMetadata']['HTTPStatusCode'] == 200:
        objecturl = "https://{pt-infra-dev-eventproc}.s3.amazonaws.com/uploads/2021_07_27-22_47_39/test-put-obj-file"
        s3uri = "s3://{0}/{1}".format(bucket, key)
        response = s3uri
    else:
        response = None
    return True, response