from dataclasses import dataclass
from typing import (
    Any,
    Literal,
    NewType,
    TypeAlias,
)

Datagram = dict[str, Any]
TestTransport: TypeAlias = "FromClient | ToClient | WaitForClosed"

StreamId = NewType("StreamId", str)
ClientId = NewType("ClientId", str)
ServerId = NewType("ServerId", str)
SessionId = NewType("SessionId", str)


@dataclass(frozen=True)
class StreamAlias:
    alias_id: int


@dataclass(frozen=True)
class ValueSet:
    seq: int
    ack: int
    from_: ServerId | None = None
    to: ClientId | None = None
    procedureName: str | None = None
    serviceName: str | None = None
    create_alias: StreamAlias | None = None
    stream_alias: StreamAlias | None = None
    payload: Datagram | None = None
    stream_closed: Literal[True] | None = None


@dataclass(frozen=True)
class FromClient:
    handshake_request: tuple[ClientId, ServerId, SessionId] | ValueSet | None = None
    stream_open: (
        tuple[ClientId, ServerId, str, str, StreamId, Datagram] | ValueSet | None
    ) = None
    stream_frame: tuple[ClientId, ServerId, int, int, Datagram] | ValueSet | None = None
    stream_closed: Literal[True] | None = None


@dataclass(frozen=True)
class ToClient:
    seq: int
    ack: int
    control_flags: int = 0
    handshake_response: bool | None = None
    stream_frame: tuple[StreamAlias, Datagram] | None = None
    stream_close: StreamAlias | None = None


@dataclass(frozen=True)
class WaitForClosed:
    pass


def decode_FromClient(datagram: dict[str, Any]) -> FromClient:
    assert "from" in datagram
    assert "to" in datagram
    if datagram.get("payload", {}).get("type") == "HANDSHAKE_REQ":
        assert "payload" in datagram
        assert "sessionId" in datagram["payload"]
        return FromClient(
            handshake_request=(
                ClientId(datagram["from"]),
                ServerId(datagram["to"]),
                SessionId(datagram["payload"]["sessionId"]),
            )
        )
    elif datagram.get("controlFlags", 0) & 0b00010 > 0:  # STREAM_OPEN_BIT
        return FromClient(
            stream_open=(
                ClientId(datagram["from"]),
                ServerId(datagram["to"]),
                datagram["serviceName"],
                datagram["procedureName"],
                StreamId(datagram["streamId"]),
                datagram["payload"],
            ),
            stream_closed=(
                datagram["controlFlags"] & 0b01000 > 0  # STREAM_CLOSED_BIT
            )
            or None,
        )
    elif datagram:
        return FromClient(
            stream_frame=(
                ClientId(datagram["from"]),
                ServerId(datagram["to"]),
                datagram["seq"],
                datagram["ack"],
                datagram["payload"],
            )
        )
    raise ValueError("Unexpected datagram: %r", datagram)


def parser(datagram: dict[str, Any]) -> FromClient:
    return decode_FromClient(datagram)
