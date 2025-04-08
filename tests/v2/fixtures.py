import asyncio
import copy
from collections import deque
from typing import (
    Any,
    AsyncGenerator,
    AsyncIterator,
    TypeAlias,
    assert_never,
)

import msgpack
import pytest
from aiochannel import Channel
from websockets.asyncio.server import ServerConnection, serve

from replit_river.transport_options import TransportOptions, UriAndMetadata
from replit_river.v2.client import Client
from tests.v2.datagrams import (
    FromClient,
    TestTransport,
    ToClient,
    WaitForClosed,
    parser,
)
from tests.v2.interpreter import make_interpreters

# Client -> Server
ClientToServerChannel: TypeAlias = Channel[Any]
# Server -> Client
ServerToClientChannel: TypeAlias = Channel[Any]


@pytest.fixture
async def raw_websocket_meta() -> AsyncIterator[
    tuple[ServerToClientChannel, ClientToServerChannel, UriAndMetadata[None]]
]:
    # server -> client
    server_to_client = Channel[dict[str, Any]](maxsize=1)
    # client -> server
    client_to_server = Channel[dict[str, Any]](maxsize=1)

    # Service websocket connection
    async def handle(websocket: ServerConnection) -> None:
        async def emit() -> None:
            async for msg in client_to_server:
                await websocket.send(msgpack.packb(msg))

        emit_task = asyncio.create_task(emit())
        try:
            while message := await websocket.recv(decode=False):
                assert isinstance(message, bytes)
                unpacked = msgpack.unpackb(message, timestamp=3)
                await server_to_client.put(unpacked)
        finally:
            server_to_client.close()
            client_to_server.close()
            emit_task.cancel()
            await emit_task
        return None

    ipv4_laddr: str | None = None
    async with serve(handle, "localhost") as server:
        for sock in server.sockets:
            if (pair := sock.getsockname())[0] == "127.0.0.1":
                ipv4_laddr = "ws://%s:%d" % pair
        serve_forever = asyncio.create_task(server.serve_forever())
        assert ipv4_laddr
        yield (
            server_to_client,
            client_to_server,
            UriAndMetadata(uri=ipv4_laddr, metadata=None),
        )

    serve_forever.cancel()


@pytest.fixture
async def bound_client(
    raw_websocket_meta: tuple[
        ClientToServerChannel,
        ServerToClientChannel,
        UriAndMetadata[None],
    ],
    expected: deque[TestTransport],
) -> AsyncGenerator[Client[None], None]:
    # Do our best to not modify the test data
    _expected = expected
    expected = copy.deepcopy(_expected)

    # client-to-server handler
    #
    # Consume FromClient events, optionally emitting datagrams to be written directly
    # back to the client.
    #
    # This represents the direct request-response flow, but does not handle
    # server-emitted events.

    client_to_server, server_to_client, uri_and_metadata = raw_websocket_meta

    async def messages_from_client() -> None:
        async for msg in client_to_server:
            parsed = parser(msg)
            try:
                next_expected = await anext(messages_from_client_channel)
            except StopAsyncIteration:
                break
            response = from_client_interpreter(received=parsed, expected=next_expected)
            if response is not None:
                await server_to_client.put(response)
            processing_finished.set()

    server_task = asyncio.create_task(messages_from_client())

    # server-to-client handler
    #
    # Consume ToClient events, optionally emitting datagrams to be written directly to
    # the client.
    #
    # This represents the other half of the "server" lifecycle, where the server can
    # choose to emit events directly without a request.

    messages_to_client_channel = Channel[ToClient]()

    async def messages_to_client() -> None:
        our_task = asyncio.current_task()
        while our_task and not our_task.cancelled():
            try:
                next_expected = await anext(messages_to_client_channel)
            except StopAsyncIteration:
                break

            response = to_client_interpreter(next_expected)
            if response is not None:
                await server_to_client.put(response)
            processing_finished.set()

    processor_task = asyncio.create_task(messages_to_client())

    # This consumes from the "expected" queue and routes messages to waiting channels
    #
    # This also handles shutdown

    async def driver() -> None:
        while expected:
            next_expected = expected.popleft()
            if isinstance(next_expected, FromClient):
                await messages_from_client_channel.put(next_expected)
            elif isinstance(next_expected, ToClient):
                await messages_to_client_channel.put(next_expected)
            elif isinstance(next_expected, WaitForClosed):
                countdown = 100
                messages_to_client_channel.close()
                while not processor_task.done():
                    if countdown <= 0:
                        break
                    countdown -= 1
                    await asyncio.sleep(0.1)
                client_to_server.close()
                await client.close()
                break
            else:
                assert_never(next_expected)
            await asyncio.sleep(0.1)
            await processing_finished.wait()

    driver_task = asyncio.create_task(driver())

    async def uri_and_metadata_factory() -> UriAndMetadata[None]:
        return uri_and_metadata

    client = Client(
        uri_and_metadata_factory=uri_and_metadata_factory,
        client_id="client-001",
        server_id="server-001",
        transport_options=TransportOptions(
            close_session_check_interval_ms=500,
        ),
    )

    from_client_interpreter, to_client_interpreter = make_interpreters()

    processing_finished = asyncio.Event()

    messages_from_client_channel = Channel[FromClient]()

    yield client

    await driver_task

    assert len(expected) == 0, "Unconsumed messages from 'expected'"
    assert messages_to_client_channel.qsize() == 0, (
        "Dangling messages the client has not consumed"
    )
    assert messages_from_client_channel.qsize() == 0, (
        "Dangling messages the processor has not consumed"
    )

    messages_to_client_channel.close()
    messages_from_client_channel.close()

    processing_finished.set()

    server_task.cancel()
    processor_task.cancel()

    await client.close()
    await server_task
    await processor_task
