import asyncio
import logging
from collections.abc import AsyncIterable, AsyncIterator
from typing import (
    Any,
    Awaitable,
    Callable,
    Coroutine,
    Generic,
    Iterable,
    Iterator,
    Literal,
    Mapping,
    NoReturn,
    Sequence,
    TypeAlias,
    TypeVar,
)

import grpc
import grpc.aio
from aiochannel import Channel, ChannelClosed
from opentelemetry.propagators.textmap import Setter
from pydantic import BaseModel, ConfigDict, Field

from replit_river.error_schema import (
    ERROR_CODE_STREAM_CLOSED,
    RiverError,
    RiverException,
    stringify_exception,
)
from replit_river.task_manager import BackgroundTaskManager
from replit_river.transport_options import (
    MAX_MESSAGE_BUFFER_SIZE,
    HandshakeMetadataType,
)

logger = logging.getLogger(__name__)

InitType = TypeVar("InitType")
RequestType = TypeVar("RequestType")
ResponseType = TypeVar("ResponseType")
ErrorType = TypeVar("ErrorType", bound=RiverError)

_MetadataType: TypeAlias = grpc.aio.Metadata | Sequence[tuple[str, str | bytes]]

GenericRpcHandler = Callable[
    [str, Channel[Any], Channel[Any]], Coroutine[None, None, None]
]
ACK_BIT = 0x0001
STREAM_OPEN_BIT = 0x0002
STREAM_CLOSED_BIT = 0x0004  # Synonymous with the cancel bit in v2

# these codes are retriable
# if the server sends a response with one of these codes,
# the client will retry the request _after_
# destroying its old session and startgin with a new one
# https://github.com/replit/river/blob/72e4fc8d02263e551d66ee5f0995707e8fa6cd1b/transport/message.ts#L83
SESSION_MISMATCH_CODE = "SESSION_STATE_MISMATCH"


# Equivalent of https://github.com/replit/river/blob/c1345f1ff6a17a841d4319fad5c153b5bda43827/transport/message.ts#L23-L33
class ExpectedSessionState(BaseModel):
    nextExpectedSeq: int
    nextSentSeq: int | None = None


class ControlMessageHandshakeRequest(BaseModel, Generic[HandshakeMetadataType]):
    type: Literal["HANDSHAKE_REQ"] = "HANDSHAKE_REQ"
    protocolVersion: str
    sessionId: str
    expectedSessionState: ExpectedSessionState
    metadata: HandshakeMetadataType | None = None


class HandShakeStatus(BaseModel):
    ok: bool
    sessionId: str | None = None
    reason: str | None = None
    code: str | None = None


class ControlMessageHandshakeResponse(BaseModel):
    type: Literal["HANDSHAKE_RESP"] = "HANDSHAKE_RESP"
    status: HandShakeStatus


class PropagationContext(BaseModel):
    traceparent: str = Field(default="")
    tracestate: str = Field(default="")


class TransportMessage(BaseModel):
    id: str
    # from_ is used instead of from because from is a reserved keyword in Python
    from_: str = Field(..., alias="from")
    to: str
    seq: int
    ack: int
    serviceName: str | None = None
    procedureName: str | None = None
    streamId: str
    controlFlags: int
    tracing: PropagationContext | None = None
    payload: Any
    model_config = ConfigDict(populate_by_name=True)
    # need this because we create TransportMessage objects with destructuring
    # where the key is "from"


class TransportMessageTracingSetter(Setter[TransportMessage]):
    """
    Handles propagating tracing context to the recipient of the message.
    """

    def set(self, carrier: TransportMessage, key: str, value: str) -> None:
        if not carrier.tracing:
            carrier.tracing = PropagationContext()
        match key:
            case "traceparent":
                carrier.tracing.traceparent = value
            case "tracestate":
                carrier.tracing.tracestate = value
            case _:
                logger.warning("unknown trace propagation key", extra={"key": key})


class GrpcContext(grpc.aio.ServicerContext, Generic[RequestType, ResponseType]):
    """Represents a gRPC-compatible ServicerContext for River interop."""

    def __init__(self, peer: str) -> None:
        self._peer = peer
        self._abort_code: grpc.StatusCode | None = None
        self._abort_details: str | None = None

    async def abort(
        self,
        code: grpc.StatusCode,
        details: str = "",
        trailing_metadata: _MetadataType = (),
    ) -> NoReturn:
        self._abort_code = code
        self._abort_details = details
        raise grpc.RpcError()

    def auth_context(self) -> Mapping[str, Iterable[bytes]]:
        return {}

    def disable_next_message_compression(self) -> None:
        # Message compression is not implemented in River.
        pass

    def invocation_metadata(self) -> None:
        return None

    def peer(self) -> str:
        return self._peer

    def peer_identities(self) -> Iterable[bytes] | None:
        return None

    def peer_identity_key(self) -> str | None:
        return None

    async def read(self) -> RequestType:
        # Normally this method should not be used.
        raise grpc.RpcError()

    async def send_initial_metadata(
        self,
        initial_metadata: _MetadataType,
    ) -> None:
        # River does not support metadata.
        pass

    def set_code(self, code: grpc.StatusCode) -> None:
        # TODO: Allow status codes to be set.
        pass

    def set_compression(self, compression: grpc.Compression) -> None:
        # Message compression is not implemented in River.
        pass

    def set_details(self, details: str) -> None:
        # TODO: Allow details to be set.
        pass

    def set_trailing_metadata(
        self,
        trailing_metadata: _MetadataType,
    ) -> None:
        # River does not support metadata.
        pass

    async def write(self, message: ResponseType) -> None:
        # Normally this method should not be used.
        raise grpc.RpcError()


def get_response_or_error_payload(
    response: Any, response_serializer: Callable[[ResponseType], Any]
) -> dict[Any, Any]:
    if isinstance(response, RiverError):
        return {
            "ok": False,
            "payload": {
                "code": response.code,
                "message": response.message,
            },
        }
    else:
        return {
            "ok": True,
            "payload": response_serializer(response),
        }


def rpc_method_handler(
    method: Callable[
        [RequestType, grpc.aio.ServicerContext], ResponseType | Awaitable[ResponseType]
    ],
    request_deserializer: Callable[[Any], RequestType],
    response_serializer: Callable[[ResponseType], Any],
) -> GenericRpcHandler:
    async def wrapped(
        peer: str,
        input: Channel[Any],
        output: Channel[Any],
    ) -> None:
        context: GrpcContext[RequestType, ResponseType] = GrpcContext(peer)
        try:
            request = request_deserializer(await input.get())
            response = method(request, context)
            if isinstance(response, Awaitable):
                response = await response
            await output.put(
                get_response_or_error_payload(response, response_serializer)
            )
        except grpc.RpcError:
            code = grpc.StatusCode(context._abort_code).name if context else "UNKNOWN"
            message = (
                f"{method.__name__} threw an exception: "
                f"{context._abort_details if context else 'Unknown error details'}"
            )
            await output.put(
                {
                    "ok": False,
                    "payload": {
                        "code": code,
                        "message": message,
                    },
                }
            )
        except Exception as e:
            logger.exception("Uncaught exception during river rpc")
            await output.put(
                {
                    "ok": False,
                    "payload": {
                        "code": "UNCAUGHT_EXCEPTION",
                        "message": (
                            f"{method.__name__} threw an "
                            f"exception: {stringify_exception(e)}"
                        ),
                    },
                }
            )
        finally:
            output.close()

    return wrapped


def subscription_method_handler(
    method: Callable[
        [RequestType, grpc.aio.ServicerContext],
        Iterable[ResponseType] | AsyncIterable[ResponseType],
    ],
    request_deserializer: Callable[[Any], RequestType],
    response_serializer: Callable[[ResponseType], Any],
) -> GenericRpcHandler:
    async def wrapped(
        peer: str,
        input: Channel[Any],
        output: Channel[Any],
    ) -> None:
        context: GrpcContext[RequestType, ResponseType] = GrpcContext(peer)
        try:
            request = request_deserializer(await input.get())
            iterator = method(request, context)
            if isinstance(iterator, AsyncIterable):
                async for response in iterator:
                    await output.put(
                        get_response_or_error_payload(response, response_serializer)
                    )
            else:
                for response in iterator:
                    await output.put(
                        get_response_or_error_payload(response, response_serializer)
                    )
        except grpc.RpcError:
            code = grpc.StatusCode(context._abort_code).name if context else "UNKNOWN"
            message = (
                f"{method.__name__} threw an exception: "
                f"{context._abort_details if context else 'Unknown error details'}"
            )
            await output.put(
                {
                    "ok": False,
                    "payload": {"code": code, "message": message},
                }
            )
        except Exception as e:
            logger.exception("Uncaught exception in river server subscription")
            await output.put(
                {
                    "ok": False,
                    "payload": {
                        "code": "UNCAUGHT_EXCEPTION",
                        "message": (
                            f"{method.__name__} threw an "
                            f"exception: {stringify_exception(e)}"
                        ),
                    },
                }
            )
        finally:
            output.close()

    return wrapped


def upload_method_handler(
    method: Callable[
        [Iterator[RequestType] | AsyncIterator[RequestType], grpc.aio.ServicerContext],
        ResponseType | Awaitable[ResponseType],
    ],
    request_deserializer: Callable[[Any], RequestType],
    response_serializer: Callable[[ResponseType], Any],
) -> GenericRpcHandler:
    async def wrapped(
        peer: str,
        input: Channel[Any],
        output: Channel[Any],
    ) -> None:
        task_manager = BackgroundTaskManager()
        try:
            context: GrpcContext[RequestType, ResponseType] = GrpcContext(peer)
            request: Channel[RequestType] = Channel(MAX_MESSAGE_BUFFER_SIZE)

            async def _convert_inputs() -> None:
                try:
                    async for item in input:
                        await request.put(request_deserializer(item))
                finally:
                    request.close()

            async def _convert_outputs() -> None:
                try:
                    response = method(request, context)
                    if isinstance(response, Awaitable):
                        response = await response
                    await output.put(
                        get_response_or_error_payload(response, response_serializer)
                    )
                except ChannelClosed:
                    raise RiverException(ERROR_CODE_STREAM_CLOSED, "Channel closed")
                except Exception as e:
                    logger.exception("Uncaught exception in river server upload")
                    await output.put(
                        {
                            "ok": False,
                            "payload": {
                                "code": "UNCAUGHT_EXCEPTION",
                                "message": (
                                    f"{method.__name__} threw an "
                                    f"exception: {stringify_exception(e)}"
                                ),
                            },
                        }
                    )
                finally:
                    output.close()

            convert_inputs_task = task_manager.create_task(_convert_inputs())
            convert_outputs_task = task_manager.create_task(_convert_outputs())
            done, _ = await asyncio.wait((convert_inputs_task, convert_outputs_task))
            for task in done:
                await task
        except Exception as e:
            logger.exception("Uncaught exception in upload")
            await output.put(
                {
                    "ok": False,
                    "payload": {
                        "code": "UNCAUGHT_EXCEPTION",
                        "message": (
                            f"{method.__name__} threw an "
                            f"exception: {stringify_exception(e)}"
                        ),
                    },
                }
            )
        finally:
            await task_manager.cancel_all_tasks()
            output.close()

    return wrapped


def stream_method_handler(
    method: Callable[
        [Iterator[RequestType] | AsyncIterator[RequestType], grpc.aio.ServicerContext],
        AsyncIterable[ResponseType],
    ],
    request_deserializer: Callable[[Any], RequestType],
    response_serializer: Callable[[ResponseType], Any],
) -> GenericRpcHandler:
    async def wrapped(
        peer: str,
        input: Channel[Any],
        output: Channel[Any],
    ) -> None:
        task_manager = BackgroundTaskManager()
        context: GrpcContext[RequestType, ResponseType] = GrpcContext(peer)
        try:
            request: Channel[RequestType] = Channel(MAX_MESSAGE_BUFFER_SIZE)

            async def _convert_inputs() -> None:
                try:
                    async for item in input:
                        await request.put(request_deserializer(item))
                finally:
                    request.close()

            response = method(request, context)

            async def _convert_outputs() -> None:
                async for item in response:
                    await output.put(
                        get_response_or_error_payload(item, response_serializer)
                    )

            convert_inputs_task = task_manager.create_task(_convert_inputs())
            convert_outputs_task = task_manager.create_task(_convert_outputs())
            done, _ = await asyncio.wait((convert_inputs_task, convert_outputs_task))
            for task in done:
                await task
        except grpc.RpcError:
            logger.exception("RPC exception in stream")
            code = grpc.StatusCode(context._abort_code).name if context else "UNKNOWN"
            message = (
                f"{method.__name__} threw an exception: "
                f"{context._abort_details if context else 'Unknown error details'}"
            )
            await output.put(
                {
                    "ok": False,
                    "payload": {
                        "code": code,
                        "message": message,
                    },
                }
            )
        except Exception as e:
            logger.exception("Uncaught exception in stream")
            await output.put(
                {
                    "ok": False,
                    "payload": {
                        "code": "UNCAUGHT_EXCEPTION",
                        "message": (
                            f"{method.__name__} threw an "
                            f"exception: {stringify_exception(e)}"
                        ),
                    },
                }
            )
        finally:
            await task_manager.cancel_all_tasks()
            output.close()

    return wrapped
