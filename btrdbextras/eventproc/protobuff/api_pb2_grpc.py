# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

import btrdbextras.eventproc.protobuff.api_pb2 as api__pb2

class EventProcessingServiceStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.ListHooks = channel.unary_unary(
                '/eventprocapi.EventProcessingService/ListHooks',
                request_serializer=api__pb2.ListHooksRequest.SerializeToString,
                response_deserializer=api__pb2.ListHooksResponse.FromString,
                )
        self.ListHandlers = channel.unary_unary(
                '/eventprocapi.EventProcessingService/ListHandlers',
                request_serializer=api__pb2.ListHandlersRequest.SerializeToString,
                response_deserializer=api__pb2.ListHandlersResponse.FromString,
                )
        self.Register = channel.unary_unary(
                '/eventprocapi.EventProcessingService/Register',
                request_serializer=api__pb2.RegisterRequest.SerializeToString,
                response_deserializer=api__pb2.RegisterResponse.FromString,
                )
        self.Deregister = channel.unary_unary(
                '/eventprocapi.EventProcessingService/Deregister',
                request_serializer=api__pb2.DeregisterRequest.SerializeToString,
                response_deserializer=api__pb2.DeregisterResponse.FromString,
                )


class EventProcessingServiceServicer(object):
    """Missing associated documentation comment in .proto file."""

    def ListHooks(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def ListHandlers(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Register(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Deregister(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_EventProcessingServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'ListHooks': grpc.unary_unary_rpc_method_handler(
                    servicer.ListHooks,
                    request_deserializer=api__pb2.ListHooksRequest.FromString,
                    response_serializer=api__pb2.ListHooksResponse.SerializeToString,
            ),
            'ListHandlers': grpc.unary_unary_rpc_method_handler(
                    servicer.ListHandlers,
                    request_deserializer=api__pb2.ListHandlersRequest.FromString,
                    response_serializer=api__pb2.ListHandlersResponse.SerializeToString,
            ),
            'Register': grpc.unary_unary_rpc_method_handler(
                    servicer.Register,
                    request_deserializer=api__pb2.RegisterRequest.FromString,
                    response_serializer=api__pb2.RegisterResponse.SerializeToString,
            ),
            'Deregister': grpc.unary_unary_rpc_method_handler(
                    servicer.Deregister,
                    request_deserializer=api__pb2.DeregisterRequest.FromString,
                    response_serializer=api__pb2.DeregisterResponse.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'eventprocapi.EventProcessingService', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class EventProcessingService(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def ListHooks(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/eventprocapi.EventProcessingService/ListHooks',
            api__pb2.ListHooksRequest.SerializeToString,
            api__pb2.ListHooksResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def ListHandlers(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/eventprocapi.EventProcessingService/ListHandlers',
            api__pb2.ListHandlersRequest.SerializeToString,
            api__pb2.ListHandlersResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def Register(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/eventprocapi.EventProcessingService/Register',
            api__pb2.RegisterRequest.SerializeToString,
            api__pb2.RegisterResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def Deregister(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/eventprocapi.EventProcessingService/Deregister',
            api__pb2.DeregisterRequest.SerializeToString,
            api__pb2.DeregisterResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
