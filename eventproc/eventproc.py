import functools
from collections import namedtuple

from eventproc.conn import connect
from eventproc.protobuff import api_pb2
from eventproc.protobuff import api_pb2_grpc

from btrdb.utils.timez import ns_to_datetime


HandlerBase = namedtuple("HandlerBase", "id name version notify_on_success notify_on_failure flags created_at created_by updated_at updated_by")

class Handler(HandlerBase):

    @classmethod
    def from_grpc(cls, h):
        return cls(
            h.id, h.name, h.version, h.notify_on_success, h.notify_on_failure,
            h.flag, ns_to_datetime(h.created_at), h.created_by, 
            ns_to_datetime(h.updated_at), h.updated_by
        )

class Service(object):

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

    def Register(self, name, hook, apikey, notify_on_success, notify_on_failure, flags):
        params = api_pb2.RegisterRequest(
            registration=api_pb2.Registration(
            name=name, 
            hook=hook, 
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


def hooks():
    conn = connect()
    s = Service(conn)
    return s.ListHooks()

def list_handlers(hook=""):
    # call grpc service to register event handler
    s = Service(connect())
    return [Handler.from_grpc(h) for h in s.ListHandlers(hook)]


def deregister(handler_id):
    # call grpc service to register event handler
    s = Service(connect())
    return s.Deregister(handler_id)
    


def register(name, hook, apikey, notify_on_success, notify_on_failure, flags=[]):
    """
    decorator to submit (register) an event handler function
    """

    # inner will actually receive the decorated func but we still have access
    # to the args & kwargs due to closure/scope.
    def inner(func):

        # call grpc service to register event handler
        s = Service(connect())
        _ = s.Register(name, hook, apikey, notify_on_success, notify_on_failure, flags)

        # return original func back to user
        return func

    return inner
