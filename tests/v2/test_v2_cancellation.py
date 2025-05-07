import asyncio
import logging
from datetime import timedelta
from typing import (
    Any,
    AsyncIterator,
    Literal,
)

import msgpack
import nanoid
import pytest

from replit_river.messages import parse_transport_msg
from replit_river.rpc import (
    STREAM_OPEN_BIT,
    ControlMessageHandshakeRequest,
    ControlMessageHandshakeResponse,
    HandShakeStatus,
    TransportMessage,
)
from replit_river.transport_options import TransportOptions
from replit_river.v2.client import Client
from replit_river.v2.session import STREAM_CANCEL_BIT, STREAM_CLOSED_BIT
from tests.v2.fixtures.raw_ws_server import OuterPayload, WsServerFixture


async def test_rpc_cancel(ws_server: WsServerFixture) -> None:
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

    sent_waiter = asyncio.Event()

    async def handle_server_messages() -> None:
        request_msg = parse_transport_msg(await recv.get())
        assert not isinstance(request_msg, str)

        logging.debug("request_msg: %r", repr(request_msg))

        assert request_msg.payload.get("payload", {}).get("hello") == "world"
        logging.debug("Found a hello:world %r", repr(request_msg))

        sent_waiter.set()

        assert request_msg.controlFlags == STREAM_OPEN_BIT | STREAM_CLOSED_BIT

        cancel_msg = parse_transport_msg(await recv.get())
        assert not isinstance(cancel_msg, str)
        assert cancel_msg.controlFlags == STREAM_CANCEL_BIT

    server_handler = asyncio.create_task(handle_server_messages())

    rpc_task = asyncio.create_task(
        client.send_rpc(
            "test",
            "cancel_rpc",
            {"ok": True, "payload": {"hello": "world"}},
            lambda x: x,
            lambda x: x,
            lambda x: x,
            timedelta(seconds=2),
        )
    )

    # Wait until we've seen at least a few messages from the upload Task
    await sent_waiter.wait()

    rpc_task.cancel()

    try:
        await rpc_task
    except asyncio.CancelledError:
        pass

    await client.close()
    await connecting

    # Ensure we're listening to close messages as well
    server_handler.cancel()
    await server_handler


@pytest.mark.parametrize("direction", ["send", "receive"])
async def test_stream_cancel(
    ws_server: WsServerFixture, direction: Literal["send", "receive"]
) -> None:
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

    bidi_waiter = asyncio.Event()

    async def send_server_messages(request_msg: TransportMessage) -> None:
        seq = 0

        while True:
            msg = TransportMessage(
                from_=request_msg.to,
                to=request_msg.from_,
                streamId=request_msg.streamId,
                controlFlags=0,
                id=nanoid.generate(),
                seq=seq,
                ack=0,
                payload={
                    "ok": True,
                    "payload": {
                        "hello": "world",
                    },
                },
            )
            seq += 1
            packed = msgpack.packb(
                msg.model_dump(by_alias=True, exclude_none=True), datetime=True
            )
            await serverconn.send(packed)
            await asyncio.sleep(0.1)

            if seq > 5 and direction == "send":
                bidi_waiter.set()

    async def handle_server_messages(request_msg: TransportMessage) -> None:
        msg = TransportMessage(**msgpack.unpackb(await recv.get()))
        while msg.payload.get("payload", {}).get("hello") == "world":
            logging.debug("Found a hello:world %r", repr(msg))
            msg = TransportMessage(**msgpack.unpackb(await recv.get()))

        assert msg.controlFlags == STREAM_CANCEL_BIT

    async def receive_chunks() -> None:
        async def _upload_chunks() -> AsyncIterator[OuterPayload[dict[Any, Any]]]:
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
                if count > 5 and direction == "receive":
                    # We've sent enough messages, interrupt the stream.
                    bidi_waiter.set()

        async for chunk in client.send_stream(
            "test",
            "cancel_stream",
            {},
            _upload_chunks(),
            lambda x: x,
            lambda x: x,
            lambda x: x,
            lambda x: x,
        ):
            print(repr(chunk))

    receive_task = asyncio.create_task(receive_chunks())
    request_msg = parse_transport_msg(await recv.get())
    logging.debug("request_msg: %r", repr(request_msg))
    assert not isinstance(request_msg, str)

    server_sender = asyncio.create_task(send_server_messages(request_msg))
    server_receiver = asyncio.create_task(handle_server_messages(request_msg))

    # Wait until we've seen at least a few messages from the requisite Task
    await bidi_waiter.wait()

    receive_task.cancel()
    try:
        await receive_task
    except asyncio.CancelledError:
        pass

    await client.close()
    await connecting

    # Ensure we're listening to close messages as well
    assert server_sender
    server_sender.cancel()
    try:
        await server_sender
    except asyncio.CancelledError:
        pass
    server_receiver.cancel()
    try:
        await server_receiver
    except Exception:
        pass


async def test_subscription_cancel(ws_server: WsServerFixture) -> None:
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

    received_waiter = asyncio.Event()

    async def handle_server_messages() -> None:
        request_msg = parse_transport_msg(await recv.get())
        assert not isinstance(request_msg, str)

        logging.debug("request_msg: %r", repr(request_msg))
        seq = 0

        while True:
            try:
                cancel_msg = parse_transport_msg(recv.get_nowait())
                break
            except asyncio.queues.QueueEmpty:
                pass

            msg = TransportMessage(
                from_=request_msg.from_,
                to=request_msg.to,
                streamId=request_msg.streamId,
                controlFlags=0,
                id=nanoid.generate(),
                seq=seq,
                ack=0,
                payload={
                    "ok": True,
                    "payload": {
                        "hello": "world",
                    },
                },
            )
            seq += 1
            packed = msgpack.packb(
                msg.model_dump(by_alias=True, exclude_none=True), datetime=True
            )
            await serverconn.send(packed)
            await asyncio.sleep(0.1)

            if seq > 5:
                received_waiter.set()

        assert not isinstance(cancel_msg, str)
        assert cancel_msg.controlFlags == STREAM_CANCEL_BIT

    server_handler = asyncio.create_task(handle_server_messages())

    async def receive_chunks() -> None:
        async for chunk in client.send_subscription(
            "test",
            "subscription_cancel",
            {},
            lambda x: x,
            lambda x: x,
            lambda x: x,
        ):
            print(repr(chunk))

    receive_task = asyncio.create_task(receive_chunks())

    # Wait until we've seen at least a few messages from the upload Task
    await received_waiter.wait()

    receive_task.cancel()
    try:
        await receive_task
    except asyncio.CancelledError:
        pass

    await client.close()
    await connecting

    # Ensure we're listening to close messages as well
    server_handler.cancel()
    await server_handler


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
            "upload_cancel",
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
