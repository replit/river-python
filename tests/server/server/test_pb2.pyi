"""
@generated by mypy-protobuf.  Do not edit manually!
isort:skip_file
import "google/protobuf/timestamp.proto";"""
import builtins
import sys
import typing

import google.protobuf.descriptor
import google.protobuf.internal.enum_type_wrapper
import google.protobuf.message

if sys.version_info >= (3, 10):
    import typing as typing_extensions
else:
    import typing_extensions

DESCRIPTOR: google.protobuf.descriptor.FileDescriptor

class _RpcEnum:
    ValueType = typing.NewType("ValueType", builtins.int)
    V: typing_extensions.TypeAlias = ValueType

class _RpcEnumEnumTypeWrapper(
    google.protobuf.internal.enum_type_wrapper._EnumTypeWrapper[_RpcEnum.ValueType],
    builtins.type,
):
    DESCRIPTOR: google.protobuf.descriptor.EnumDescriptor
    ZERO: _RpcEnum.ValueType  # 0
    ONE: _RpcEnum.ValueType  # 1
    TWO: _RpcEnum.ValueType  # 2

class RpcEnum(_RpcEnum, metaclass=_RpcEnumEnumTypeWrapper): ...

ZERO: RpcEnum.ValueType  # 0
ONE: RpcEnum.ValueType  # 1
TWO: RpcEnum.ValueType  # 2
global___RpcEnum = RpcEnum

@typing_extensions.final
class RpcMessage(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    PAYLOAD_FIELD_NUMBER: builtins.int
    payload: builtins.str
    def __init__(
        self,
        *,
        payload: builtins.str = ...,
    ) -> None: ...
    def ClearField(
        self, field_name: typing_extensions.Literal["payload", b"payload"]
    ) -> None: ...

global___RpcMessage = RpcMessage

@typing_extensions.final
class RpcRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    DOUBLE_FIELD_NUMBER: builtins.int
    FLOAT_FIELD_NUMBER: builtins.int
    INT64_FIELD_NUMBER: builtins.int
    UINT64_FIELD_NUMBER: builtins.int
    INT32_FIELD_NUMBER: builtins.int
    FIXED64_FIELD_NUMBER: builtins.int
    FIXED32_FIELD_NUMBER: builtins.int
    BOOL_FIELD_NUMBER: builtins.int
    STRING_FIELD_NUMBER: builtins.int
    MESSAGE_FIELD_NUMBER: builtins.int
    BYTES_FIELD_NUMBER: builtins.int
    UINT32_FIELD_NUMBER: builtins.int
    ENUM_FIELD_NUMBER: builtins.int
    SFIXED32_FIELD_NUMBER: builtins.int
    SFIXED64_FIELD_NUMBER: builtins.int
    SINT32_FIELD_NUMBER: builtins.int
    SINT64_FIELD_NUMBER: builtins.int
    double: builtins.float
    float: builtins.float
    int64: builtins.int
    uint64: builtins.int
    int32: builtins.int
    fixed64: builtins.int
    fixed32: builtins.int
    bool: builtins.bool
    string: builtins.str
    @property
    def message(self) -> global___RpcMessage:
        """No group."""
    bytes: builtins.bytes
    uint32: builtins.int
    enum: global___RpcEnum.ValueType
    sfixed32: builtins.int
    sfixed64: builtins.int
    sint32: builtins.int
    sint64: builtins.int
    def __init__(
        self,
        *,
        double: builtins.float = ...,
        float: builtins.float = ...,
        int64: builtins.int = ...,
        uint64: builtins.int = ...,
        int32: builtins.int = ...,
        fixed64: builtins.int = ...,
        fixed32: builtins.int = ...,
        bool: builtins.bool = ...,
        string: builtins.str = ...,
        message: global___RpcMessage | None = ...,
        bytes: builtins.bytes = ...,
        uint32: builtins.int = ...,
        enum: global___RpcEnum.ValueType = ...,
        sfixed32: builtins.int = ...,
        sfixed64: builtins.int = ...,
        sint32: builtins.int = ...,
        sint64: builtins.int = ...,
    ) -> None: ...
    def HasField(
        self,
        field_name: typing_extensions.Literal[
            "bool",
            b"bool",
            "bytes",
            b"bytes",
            "double",
            b"double",
            "enum",
            b"enum",
            "fixed32",
            b"fixed32",
            "fixed64",
            b"fixed64",
            "float",
            b"float",
            "int32",
            b"int32",
            "int64",
            b"int64",
            "message",
            b"message",
            "request",
            b"request",
            "sfixed32",
            b"sfixed32",
            "sfixed64",
            b"sfixed64",
            "sint32",
            b"sint32",
            "sint64",
            b"sint64",
            "string",
            b"string",
            "uint32",
            b"uint32",
            "uint64",
            b"uint64",
        ],
    ) -> builtins.bool: ...
    def ClearField(
        self,
        field_name: typing_extensions.Literal[
            "bool",
            b"bool",
            "bytes",
            b"bytes",
            "double",
            b"double",
            "enum",
            b"enum",
            "fixed32",
            b"fixed32",
            "fixed64",
            b"fixed64",
            "float",
            b"float",
            "int32",
            b"int32",
            "int64",
            b"int64",
            "message",
            b"message",
            "request",
            b"request",
            "sfixed32",
            b"sfixed32",
            "sfixed64",
            b"sfixed64",
            "sint32",
            b"sint32",
            "sint64",
            b"sint64",
            "string",
            b"string",
            "uint32",
            b"uint32",
            "uint64",
            b"uint64",
        ],
    ) -> None: ...
    def WhichOneof(
        self, oneof_group: typing_extensions.Literal["request", b"request"]
    ) -> (
        typing_extensions.Literal[
            "double",
            "float",
            "int64",
            "uint64",
            "int32",
            "fixed64",
            "fixed32",
            "bool",
            "string",
            "message",
            "bytes",
            "uint32",
            "enum",
            "sfixed32",
            "sfixed64",
            "sint32",
            "sint64",
        ]
        | None
    ): ...

global___RpcRequest = RpcRequest

@typing_extensions.final
class RpcResponse(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    PAYLOAD_FIELD_NUMBER: builtins.int
    payload: builtins.str
    def __init__(
        self,
        *,
        payload: builtins.str = ...,
    ) -> None: ...
    def ClearField(
        self, field_name: typing_extensions.Literal["payload", b"payload"]
    ) -> None: ...

global___RpcResponse = RpcResponse

@typing_extensions.final
class StreamRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    PAYLOAD_FIELD_NUMBER: builtins.int
    payload: builtins.str
    """TODO: timestamp is causing grpc generated files fail in tests.
    google.protobuf.Timestamp timestamp = 1;
    """
    def __init__(
        self,
        *,
        payload: builtins.str = ...,
    ) -> None: ...
    def ClearField(
        self, field_name: typing_extensions.Literal["payload", b"payload"]
    ) -> None: ...

global___StreamRequest = StreamRequest

@typing_extensions.final
class StreamResponse(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    PAYLOAD_FIELD_NUMBER: builtins.int
    payload: builtins.str
    """google.protobuf.Timestamp timestamp = 1;"""
    def __init__(
        self,
        *,
        payload: builtins.str = ...,
    ) -> None: ...
    def ClearField(
        self, field_name: typing_extensions.Literal["payload", b"payload"]
    ) -> None: ...

global___StreamResponse = StreamResponse
