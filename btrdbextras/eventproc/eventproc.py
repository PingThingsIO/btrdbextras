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
import os
import warnings
import uuid
from collections import namedtuple

import dill
import grpc
import certifi
from btrdb.utils.timez import ns_to_datetime
from btrdbextras.eventproc.protobuff import api_pb2
from btrdbextras.eventproc.protobuff import api_pb2_grpc

__all__ = ['hooks', 'list_handlers', 'register', 'deregister', 'upload_file', '_uploads']
_uploads = {}

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

    # grpc bundles its own CA certs which will work for all normal SSL
    # certificates but will fail for custom CA certs. Allow the user
    # to specify a CA bundle via env var to overcome this
    env_bundle = os.getenv("BTRDB_CA_BUNDLE", "")

    # certifi certs are provided as part of this package install
    # https://github.com/certifi/python-certifi
    lib_certs = certifi.where()

    ca_bundle = env_bundle

    if ca_bundle == "":
        ca_bundle = lib_certs
    try:
        with open(ca_bundle, "rb") as f:
            contents = f.read()
    except Exception:
        if env_bundle != "":
            # The user has given us something but we can't use it, we need to make noise
            raise Exception("BTRDB_CA_BUNDLE(%s) env is defined but could not read file" % ca_bundle)
        else:
            contents = None

    return grpc.secure_channel(
        endpoint,
        grpc.composite_channel_credentials(
            grpc.ssl_channel_credentials(contents),
            grpc.access_token_call_credentials(apikey)
        )
    )


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
    Uploads file to S3. Returns a link to download the file.
    If the function runs outside of an eventproc handler executing in response to
    a hook, it will just check the inputs, raise a warning, and return None.

    Parameters
    ----------
    file: string
        Path to the file.
    file_name: string
        Name that the file will be called on download. Maximum 200 characters.

    Raises
    ---------
    TypeError: file must be a string.
    TypeError: file_name must be a string.
    ValueError: file must be a path to a file, relative to the home directory.
    ValueError: file_name cannot be longer than 200 characters, is <actual length>.
    
    Returns
    ----------
    string: Download link to the object. None if upload was not attempted.
    """
    
    # check the inputs
    if not isinstance(file, str):
        raise TypeError("file must be a string.")
    if not isinstance(file_name, str):
        raise TypeError("file_name must be a string.")
    if not os.path.exists(file):
        raise ValueError("file must be a path to a file.")
    if len(file_name) > 200:
        raise ValueError("file_name cannot be longer than 200 characters, is {0}.".format(len(file_name)))

    # check the context
    if not os.getenv("EXECUTOR_CONTEXT") == "true":
        m = "upload_file is running in an execution context without the appropriate AWS credentials and will not upload to S3."
        warnings.warn(m)
        return None
        
    # queue the upload, to be completed by the executor when the handler completes
    code = str(uuid.uuid4().hex)
    _uploads[code] = [file, file_name]
    
    return "https://{0}/{1}".format(os.getenv("DOWNLOADS_ENDPOINT"), code)