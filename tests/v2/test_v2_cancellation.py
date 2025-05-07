import asyncio
import logging
from typing import (
    Any,
    AsyncIterator,
    Literal,
    TypedDict,
)

import msgpack
import nanoid

from replit_river.messages import parse_transport_msg
from replit_river.rpc import (
    ControlMessageHandshakeRequest,
    ControlMessageHandshakeResponse,
    HandShakeStatus,
    TransportMessage,
)
from replit_river.transport_options import TransportOptions
from replit_river.v2.client import Client
from replit_river.v2.session import STREAM_CANCEL_BIT
from tests.v2.fixtures.raw_ws_server import WsServerFixture


class OuterPayload[A](TypedDict):
    ok: Literal[True]
    payload: A


async def test_upload_cancel(ws_server: WsServerFixture) -> None:
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

        logging.debug("request_msg: %r", repr(request_msg))

        msg = TransportMessage(**msgpack.unpackb(await recv.get()))
        while msg.payload.get("payload", {}).get("hello") == "world":
            logging.debug("Found a hello:world %r", repr(msg))
            msg = TransportMessage(**msgpack.unpackb(await recv.get()))

        assert msg.controlFlags == STREAM_CANCEL_BIT

    server_handler = asyncio.create_task(handle_server_messages())

    sent_waiter = asyncio.Event()

    async def upload_chunks() -> AsyncIterator[OuterPayload[dict[Any, Any]]]:
        count = 0
        while True:
            await asyncio.sleep(0.1)
            yield {
                "ok": True,
                "payload": {
                    "hello": "world",
                },
            }
            count += 1
            if count > 5:
                # We've sent enough messages, interrupt the stream.
                sent_waiter.set()

    upload_task = asyncio.create_task(
        client.send_upload(
            "test",
            "bigstream",
            {},
            upload_chunks(),
            lambda x: x,
            lambda x: x,
            lambda x: x,
            lambda x: x,
        )
    )

    # Wait until we've seen at least a few messages from the upload Task
    await sent_waiter.wait()

    upload_task.cancel()
    try:
        await upload_task
    except asyncio.CancelledError:
        pass

    await client.close()
    await connecting

    # Ensure we're listening to close messages as well
    server_handler.cancel()
    await server_handler
