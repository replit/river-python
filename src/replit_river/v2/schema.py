from typing import Any, Literal, NotRequired, TypeAlias, TypedDict

from grpc.aio import BaseError

from replit_river.rpc import ACK_BIT, ACK_BIT_TYPE, ExpectedSessionState
from replit_river.v2.client_session import STREAM_CANCEL_BIT, STREAM_CANCEL_BIT_TYPE


class ControlClose(TypedDict):
    type: Literal["CLOSE"]


class ControlAck(TypedDict):
    type: Literal["ACK"]


class ControlHandshakeRequest(TypedDict):
    type: Literal["HANDSHAKE_REQ"]
    protocolVersion: Literal["v2.0"]
    sessionId: str
    expectedSessionState: ExpectedSessionState
    metdata: NotRequired[Any]


class HandshakeOK(TypedDict):
    ok: Literal[True]
    sessionId: str


class HandshakeError(TypedDict):
    ok: Literal[False]
    reaason: str


class ControlHandshakeResponse(TypedDict):
    type: Literal["HANDSHAKE_RESP"]
    status: HandshakeOK | HandshakeError

# This is sent when the server encounters an internal error
# i.e. an invariant has been violated
class BaseErrorStructure(TypedDict):
  # This should be a defined literal to make sure errors are easily differentiated
  # code: str  # Supplied by implementations
  # This can be any string
  message: str
  # Any extra metadata
  extra: NotRequired[Any]

# When a client sends a malformed request. This can be
# for a variety of reasons which would  be included
# in the message.
class InvalidRequestError(BaseErrorStructure):
  code: Literal['INVALID_REQUEST']

# This is sent when an exception happens in the handler of a stream.
class UncaughtError(BaseErrorStructure):
  code: Literal['UNCAUGHT_ERROR']

# This is sent when one side wishes to cancel the stream
# abruptly from user-space. Handling this is up to the procedure
# implementation or the caller.
class CancelError(BaseErrorStructure):
  code: Literal['CANCEL']

ProtocolError: TypeAlias = UncaughtError | InvalidRequestError | CancelError;

Control: TypeAlias = (
    ControlClose | ControlAck | ControlHandshakeRequest | ControlHandshakeResponse
)

ValidPairings = (
    tuple[ACK_BIT_TYPE, ControlAck] |
    tuple[STREAM_CANCEL_BIT_TYPE, ProtocolError]
)
