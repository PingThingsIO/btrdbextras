import functools

from eventproc.conn import connect
from eventproc.protobuff import api_pb2
from eventproc.protobuff import api_pb2_grpc





class Service(object):

    def __init__(self, channel):
        self.channel = channel
        self.stub = api_pb2_grpc.EventProcessingServiceStub(channel)

    def ListHooks(self):
        params = api_pb2.ListHooksRequest()
        return [r.name for r in self.stub.ListHooks(params).hooks]

    def ListHandlers(self):
        params = api_pb2.ListHandlersRequest()
        for result in self.stub.ListHandlers(params):
            yield result




def hooks():
    conn = connect()
    s = Service(conn)
    return s.ListHooks()


# def register(func):

#     @functools.wraps(func)
#     def wrapper(*args, **kwargs):
#         func(*args, **kwargs)
#         return func(*args, **kwargs)
#     return wrapper


# def foo(x=1):
#     def wrap(f):
#         def f_foo(*args, **kw):
#             # do something to f
#             return f(*args, **kw)
#         return f_foo
#     return wrap

