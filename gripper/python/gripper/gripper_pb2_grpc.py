# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

from gripper import gripper_pb2 as gripper_dot_gripper__pb2


class GRIPSourceStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.GetCollections = channel.unary_stream(
                '/gripper.GRIPSource/GetCollections',
                request_serializer=gripper_dot_gripper__pb2.Empty.SerializeToString,
                response_deserializer=gripper_dot_gripper__pb2.Collection.FromString,
                )
        self.GetCollectionInfo = channel.unary_unary(
                '/gripper.GRIPSource/GetCollectionInfo',
                request_serializer=gripper_dot_gripper__pb2.Collection.SerializeToString,
                response_deserializer=gripper_dot_gripper__pb2.CollectionInfo.FromString,
                )
        self.GetIDs = channel.unary_stream(
                '/gripper.GRIPSource/GetIDs',
                request_serializer=gripper_dot_gripper__pb2.Collection.SerializeToString,
                response_deserializer=gripper_dot_gripper__pb2.RowID.FromString,
                )
        self.GetRows = channel.unary_stream(
                '/gripper.GRIPSource/GetRows',
                request_serializer=gripper_dot_gripper__pb2.Collection.SerializeToString,
                response_deserializer=gripper_dot_gripper__pb2.Row.FromString,
                )
        self.GetRowsByID = channel.stream_stream(
                '/gripper.GRIPSource/GetRowsByID',
                request_serializer=gripper_dot_gripper__pb2.RowRequest.SerializeToString,
                response_deserializer=gripper_dot_gripper__pb2.Row.FromString,
                )
        self.GetRowsByField = channel.unary_stream(
                '/gripper.GRIPSource/GetRowsByField',
                request_serializer=gripper_dot_gripper__pb2.FieldRequest.SerializeToString,
                response_deserializer=gripper_dot_gripper__pb2.Row.FromString,
                )


class GRIPSourceServicer(object):
    """Missing associated documentation comment in .proto file."""

    def GetCollections(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetCollectionInfo(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetIDs(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetRows(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetRowsByID(self, request_iterator, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetRowsByField(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_GRIPSourceServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'GetCollections': grpc.unary_stream_rpc_method_handler(
                    servicer.GetCollections,
                    request_deserializer=gripper_dot_gripper__pb2.Empty.FromString,
                    response_serializer=gripper_dot_gripper__pb2.Collection.SerializeToString,
            ),
            'GetCollectionInfo': grpc.unary_unary_rpc_method_handler(
                    servicer.GetCollectionInfo,
                    request_deserializer=gripper_dot_gripper__pb2.Collection.FromString,
                    response_serializer=gripper_dot_gripper__pb2.CollectionInfo.SerializeToString,
            ),
            'GetIDs': grpc.unary_stream_rpc_method_handler(
                    servicer.GetIDs,
                    request_deserializer=gripper_dot_gripper__pb2.Collection.FromString,
                    response_serializer=gripper_dot_gripper__pb2.RowID.SerializeToString,
            ),
            'GetRows': grpc.unary_stream_rpc_method_handler(
                    servicer.GetRows,
                    request_deserializer=gripper_dot_gripper__pb2.Collection.FromString,
                    response_serializer=gripper_dot_gripper__pb2.Row.SerializeToString,
            ),
            'GetRowsByID': grpc.stream_stream_rpc_method_handler(
                    servicer.GetRowsByID,
                    request_deserializer=gripper_dot_gripper__pb2.RowRequest.FromString,
                    response_serializer=gripper_dot_gripper__pb2.Row.SerializeToString,
            ),
            'GetRowsByField': grpc.unary_stream_rpc_method_handler(
                    servicer.GetRowsByField,
                    request_deserializer=gripper_dot_gripper__pb2.FieldRequest.FromString,
                    response_serializer=gripper_dot_gripper__pb2.Row.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'gripper.GRIPSource', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class GRIPSource(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def GetCollections(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_stream(request, target, '/gripper.GRIPSource/GetCollections',
            gripper_dot_gripper__pb2.Empty.SerializeToString,
            gripper_dot_gripper__pb2.Collection.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def GetCollectionInfo(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/gripper.GRIPSource/GetCollectionInfo',
            gripper_dot_gripper__pb2.Collection.SerializeToString,
            gripper_dot_gripper__pb2.CollectionInfo.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def GetIDs(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_stream(request, target, '/gripper.GRIPSource/GetIDs',
            gripper_dot_gripper__pb2.Collection.SerializeToString,
            gripper_dot_gripper__pb2.RowID.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def GetRows(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_stream(request, target, '/gripper.GRIPSource/GetRows',
            gripper_dot_gripper__pb2.Collection.SerializeToString,
            gripper_dot_gripper__pb2.Row.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def GetRowsByID(request_iterator,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.stream_stream(request_iterator, target, '/gripper.GRIPSource/GetRowsByID',
            gripper_dot_gripper__pb2.RowRequest.SerializeToString,
            gripper_dot_gripper__pb2.Row.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def GetRowsByField(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_stream(request, target, '/gripper.GRIPSource/GetRowsByField',
            gripper_dot_gripper__pb2.FieldRequest.SerializeToString,
            gripper_dot_gripper__pb2.Row.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
