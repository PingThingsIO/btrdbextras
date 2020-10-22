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

from btrdbextras.eventproc.conn import connect
from btrdbextras.eventproc.protobuff import api_pb2
from btrdbextras.eventproc.protobuff import api_pb2_grpc


__all__ = ['hooks', 'list_handlers', 'register', 'deregister']


##########################################################################
## Helper Classes
##########################################################################

HandlerBase = namedtuple("HandlerBase", "id name version notify_on_success notify_on_failure flags created_at created_by updated_at updated_by")

class Handler(HandlerBase):
    """
    Class definition for an event handler object.  Inherits from HandlerBase
    to add methods to the namedtuple.
    """

    @classmethod
    def from_grpc(cls, h):
        return cls(
            h.id, h.name, h.version, h.notify_on_success, h.notify_on_failure,
            h.flag, ns_to_datetime(h.created_at), h.created_by,
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

    def Register(self, name, hook, func, apikey, notify_on_success, notify_on_failure, flags):
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
            flags=flags,
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

def hooks():
    """
    List registered hooks.
    """
    conn = connect()
    s = Service(conn)
    return s.ListHooks()

def list_handlers(hook=""):
    """
    List registered handlers.  An optional hook name is allowed to filter
    results.

    Parameters
    ----------
    hook: str
        Optional hook name to filter registered handlers.

    """
    s = Service(connect())
    return [Handler.from_grpc(h) for h in s.ListHandlers(hook)]


def deregister(handler_id):
    """
    Removes and existing event handler by ID.

    Parameters
    ----------
    id: int
        ID of the event handler to remove.

    """
    s = Service(connect())
    return s.Deregister(handler_id)



def register(name, hook, apikey, notify_on_success, notify_on_failure, flags=[]):
    """
    decorator to submit (register) an event handler function

    Parameters
    ----------
    name: str
        Friendly name of this event handler for display purposes.
    hook: str
        Name of the hook that this event handler responds to.
    apikey: str
        BTrDB api key to use for context/secure access when the event handler
        executes.
    notify_on_success: str
        Email address of user to notify when event handler completes successfully.
    notify_on_failure: str
        Email address of user to notify when event handler does not complete
        successfully.
    flags: list of str
        Filtering flags that users can choose when identifying handlers to
        execute.

    """

    # inner will actually receive the decorated func but we still have access
    # to the args & kwargs due to closure/scope.
    def inner(func):

        # call grpc service to register event handler
        s = Service(connect())
        _ = s.Register(name, hook, func, apikey, notify_on_success, notify_on_failure, flags)

        # return original func back to user
        return func

    return inner
