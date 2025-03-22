from typing import Any, Literal, NotRequired, TypeAlias, TypedDict

from replit_river.rpc import ExpectedSessionState


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


Control: TypeAlias = (
    ControlClose | ControlAck | ControlHandshakeRequest | ControlHandshakeResponse
)
