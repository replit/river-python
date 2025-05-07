import asyncio
import logging

import msgpack
import nanoid

from replit_river.common_session import SessionState
from replit_river.messages import parse_transport_msg
from replit_river.rate_limiter import RateLimiter
from replit_river.rpc import (
    ControlMessageHandshakeRequest,
    ControlMessageHandshakeResponse,
    HandShakeStatus,
    TransportMessage,
)
from replit_river.transport_options import TransportOptions
from replit_river.v2.client import Client
from replit_river.v2.session import STREAM_CLOSED_BIT, Session
from tests.v2.fixtures.raw_ws_server import WsServerFixture


class _PermissiveRateLimiter(RateLimiter):
    def start_restoring_budget(self, user: str) -> None:
        pass

    def get_backoff_ms(self, user: str) -> float:
        return 0

    def has_budget(self, user: str) -> bool:
        return True

    def consume_budget(self, user: str) -> None:
        pass


async def test_connect(ws_server: WsServerFixture) -> None:
    (urimeta, recv, conn) = ws_server

    session = Session(
        server_id="SERVER",
        session_id="SESSION1",
        transport_options=TransportOptions(),
        close_session_callback=lambda _: None,
        client_id="CLIENT1",
        rate_limiter=_PermissiveRateLimiter(),
        uri_and_metadata_factory=urimeta,
    )

    connecting = asyncio.create_task(session.ensure_connected())
    msg = parse_transport_msg(await recv.get())
    assert isinstance(msg, TransportMessage)
    assert msg.payload["type"] == "HANDSHAKE_REQ"
    await session.close()
    await connecting


async def test_close_race(ws_server: WsServerFixture) -> None:
    (urimeta, recv, conn) = ws_server

    callcount = 0

    def close_session_callback(_session: Session) -> None:
        nonlocal callcount
        callcount += 1

    session = Session(
        server_id="SERVER",
        session_id="SESSION1",
        transport_options=TransportOptions(),
        close_session_callback=close_session_callback,
        client_id="CLIENT1",
        rate_limiter=_PermissiveRateLimiter(),
        uri_and_metadata_factory=urimeta,
    )

    connecting = asyncio.create_task(session.ensure_connected())
    msg = parse_transport_msg(await recv.get())
    assert isinstance(msg, TransportMessage)
    assert msg.payload["type"] == "HANDSHAKE_REQ"
    await session.close()
    await session.close()
    await session.close()
    await session.close()
    await connecting
    assert session._state == SessionState.CLOSED
    assert callcount == 1


async def test_big_packet(ws_server: WsServerFixture) -> None:
    (urimeta, recv, conn) = ws_server

    client = Client(
        client_id="CLIENT1",
        server_id="SERVER",
        transport_options=TransportOptions(),
        uri_and_metadata_factory=urimeta,
    )

    connecting = asyncio.create_task(client.ensure_connected())
    request_msg = parse_transport_msg(await recv.get())

    assert not isinstance(request_msg, str)
    assert (serverconn := conn())
    handshake_request: ControlMessageHandshakeRequest[None] = (
        ControlMessageHandshakeRequest(**request_msg.payload)
    )

    handshake_resp = ControlMessageHandshakeResponse(
        status=HandShakeStatus(
            ok=True,
        ),
    )
    handshake_request.sessionId

    msg = TransportMessage(
        from_=request_msg.from_,
        to=request_msg.to,
        streamId=request_msg.streamId,
        controlFlags=0,
        id=nanoid.generate(),
        seq=0,
        ack=0,
        payload=handshake_resp.model_dump(),
    )
    packed = msgpack.packb(
        msg.model_dump(by_alias=True, exclude_none=True), datetime=True
    )
    await serverconn.send(packed)

    async def handle_server_messages() -> None:
        request_msg = parse_transport_msg(await recv.get())
        assert not isinstance(request_msg, str)
        msg = TransportMessage(
            from_=request_msg.to,
            to=request_msg.from_,
            streamId=request_msg.streamId,
            controlFlags=STREAM_CLOSED_BIT,
            id=nanoid.generate(),
            seq=0,
            ack=0,
            payload={
                "ok": True,
                "payload": {
                    "big": "a" * (2**20 + 1),  # One more than the default max_size
                },
            },
        )

        packed = msgpack.packb(
            msg.model_dump(by_alias=True, exclude_none=True), datetime=True
        )
        await serverconn.send(packed)

        stream_close_msg = msgpack.unpackb(await recv.get())
        assert stream_close_msg["controlFlags"] == STREAM_CLOSED_BIT

    server_handler = asyncio.create_task(handle_server_messages())

    try:
        async for datagram in client.send_subscription(
            "test", "bigstream", {}, lambda x: x, lambda x: x, lambda x: x
        ):
            print(datagram)
    except Exception:
        logging.exception("Interrupted")

    await client.close()
    await connecting

    # Ensure we're listening to close messages as well
    server_handler.cancel()
    await server_handler
