from typing import (
    Any,
    Callable,
    NotRequired,
    Protocol,
    TypedDict,
)

import nanoid

from tests.v2.datagrams import (
    ClientId,
    FromClient,
    ServerId,
    StreamAlias,
    StreamId,
    ToClient,
    ValueSet,
)

Datagram = dict[str, Any]


class FromClientInterpreter(Protocol):
    def __call__(
        self, *, received: FromClient, expected: FromClient
    ) -> Datagram | None: ...


class _TestTransportState(TypedDict):
    streams: dict[StreamAlias, tuple[ClientId, ServerId, StreamId]]
    client_id: NotRequired[str]
    server_id: NotRequired[str]


def make_interpreters() -> tuple[
    FromClientInterpreter,
    Callable[[ToClient], Datagram | None],
]:
    state: _TestTransportState = {
        "streams": {},
    }

    def from_client_interpreter(
        received: FromClient,
        expected: FromClient,
    ) -> Datagram | None:
        if isinstance(received.handshake_request, tuple):
            (from_, to, session_id) = received.handshake_request
            assert isinstance(expected.handshake_request, ValueSet), (
                f"Expected ValueSet 1: {repr(received)}, {repr(expected)}"
            )
            assert expected.handshake_request.from_, (
                f"Expected {expected.handshake_request.from_}"
            )
            assert expected.handshake_request.to, (
                "Expected {expected.handshake_request.to}"
            )
            assert expected.handshake_request.from_ == to, (
                f"Expected {expected.handshake_request.from_} == {to}"
            )
            assert expected.handshake_request.to == from_, (
                "Expected {expected.handshake_request.to} == {from_}"
            )
            return _build_datagram(
                seq=expected.handshake_request.seq,
                packet_id=nanoid.generate(),
                stream_id=nanoid.generate(),
                control_flags=0,
                ack=expected.handshake_request.ack,
                client_id=expected.handshake_request.from_,
                server_id=expected.handshake_request.to,
                payload=_build_handshake_resp(session_id),
            )
        elif isinstance(received.stream_open, tuple):
            (from_, to, service_name, procedure_name, stream_id, payload) = (
                received.stream_open
            )
            assert isinstance(expected.stream_open, ValueSet), "Expected ValueSet 2"
            assert expected.stream_open.from_ == to, (
                f"Expected {expected.stream_open.from_} == {to}"
            )
            assert expected.stream_open.to == from_, (
                "Expected {expected.stream_open.to} == {from_}"
            )
            assert expected.stream_open.serviceName == service_name, (
                f"Expected {expected.stream_open.serviceName} == {service_name}"
            )
            assert expected.stream_open.procedureName == procedure_name, (
                f"Expected {expected.stream_open.procedureName} == {procedure_name}"
            )
            assert expected.stream_open.create_alias, (
                "Expected create_alias to be a StreamAlias"
            )
            if expected.stream_open.payload is not None and payload is not None:
                assert expected.stream_open.payload == payload, (
                    f"Expected {expected.stream_open.payload} == {payload}"
                )
            assert expected.stream_open.stream_closed or not received.stream_closed, (
                f"Are we self-closing? {expected.stream_open.stream_closed} "
                f"or not {received.stream_closed}"
            )
            # Do it all again because mypy can't infer correctly
            alias_mapping: tuple[ClientId, ServerId, StreamId] = (
                ClientId(from_),
                ServerId(to),
                StreamId(stream_id),
            )
            state["streams"][expected.stream_open.create_alias] = alias_mapping
            return None
        elif isinstance(received.stream_frame, tuple):
            (from_, to, seq, ack, payload) = received.stream_frame
            assert isinstance(expected.stream_frame, ValueSet), "Expected ValueSet 3"
            assert seq == expected.stream_frame.seq, (
                f"Expected seq {seq} == {expected.stream_frame.seq}"
            )
            assert ack == expected.stream_frame.ack, (
                f"Expected ack {ack} == {expected.stream_frame.ack}"
            )
            assert expected.stream_frame.stream_alias, "Expected stream_alias"
            (from_, to, stream_id) = state["streams"][
                expected.stream_frame.stream_alias
            ]
            assert expected.stream_frame.payload == payload, (
                f"Expected {expected.stream_frame.payload} == {payload}"
            )
            return None
        raise ValueError("Unexpected from_client case: %r", received)

    def to_client_interpreter(expected: ToClient) -> Datagram | None:
        if expected.stream_frame is not None:
            (stream_alias, payload) = expected.stream_frame
            (from_, to, stream_id) = state["streams"][stream_alias]
            return _build_datagram(
                seq=expected.seq,
                ack=expected.ack,
                control_flags=expected.control_flags,
                packet_id=nanoid.generate(),
                stream_id=stream_id,
                client_id=from_,
                server_id=to,
                payload=payload,
            )
        elif expected.stream_close is not None:
            stream_alias = expected.stream_close
            (from_, to, stream_id) = state["streams"][stream_alias]
            return _build_datagram(
                seq=expected.seq,
                ack=expected.ack,
                control_flags=0b01000,
                packet_id=nanoid.generate(),
                stream_id=stream_id,
                client_id=from_,
                server_id=to,
                payload={"type": "CLOSE"},
            )
        raise ValueError("Unexpected to_client case: %r", expected)

    return from_client_interpreter, to_client_interpreter


def _strip_none(datagram: Datagram) -> Datagram:
    return {k: v for k, v in datagram.items() if v is not None}


def _build_handshake_resp(session_id: str) -> Datagram:
    return _strip_none(
        {
            "type": "HANDSHAKE_RESP",
            "status": {
                "ok": True,
                "sessionId": session_id,
            },
        }
    )


def _build_datagram(
    *,
    packet_id: str,
    stream_id: str,
    server_id: str,
    client_id: str,
    control_flags: int,
    seq: int,
    ack: int,
    payload: Datagram,
) -> Datagram:
    return _strip_none(
        {
            "id": packet_id,
            "from": server_id,
            "to": client_id,
            "seq": seq,
            "ack": ack,
            "streamId": stream_id,
            "controlFlags": control_flags,
            "payload": payload,
        }
    )
