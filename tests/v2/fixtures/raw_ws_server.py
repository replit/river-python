import asyncio
from typing import (
    AsyncIterator,
    Awaitable,
    Callable,
    Literal,
    TypeAlias,
    TypedDict,
)

import pytest
from websockets import ConnectionClosed, ConnectionClosedOK, Data
from websockets.asyncio.server import ServerConnection, serve

from replit_river.transport_options import UriAndMetadata

WsServerFixture: TypeAlias = tuple[
    Callable[[], Awaitable[UriAndMetadata[None]]],
    asyncio.Queue[bytes],
    Callable[[], ServerConnection | None],
]


class OuterPayload[A](TypedDict):
    ok: Literal[True]
    payload: A


class _WsServerState(TypedDict):
    ipv4_laddr: tuple[str, int] | None


async def _ws_server_internal(
    recv: asyncio.Queue[bytes],
    set_conn: Callable[[ServerConnection], None],
    state: _WsServerState,
) -> AsyncIterator[None]:
    async def handle(websocket: ServerConnection) -> None:
        set_conn(websocket)
        datagram: Data
        try:
            while datagram := await websocket.recv(decode=False):
                if isinstance(datagram, str):
                    continue
                await recv.put(datagram)
        except ConnectionClosedOK:
            pass
        except ConnectionClosed:
            pass

    port: int | None = None
    if state["ipv4_laddr"]:
        port = state["ipv4_laddr"][1]
    async with serve(handle, "localhost", port=port) as server:
        for sock in server.sockets:
            if (pair := sock.getsockname())[0] == "127.0.0.1":
                if state["ipv4_laddr"] is None:
                    state["ipv4_laddr"] = pair
        serve_forever = asyncio.create_task(server.serve_forever())
        yield None
        server.close()
        await server.wait_closed()
        # "serve_forever" should always be done after wait_closed finishes
        assert serve_forever.done()


@pytest.fixture
async def ws_server() -> AsyncIterator[WsServerFixture]:
    recv: asyncio.Queue[bytes] = asyncio.Queue(maxsize=1)
    connection: ServerConnection | None = None
    state: _WsServerState = {"ipv4_laddr": None}

    def set_conn(new_conn: ServerConnection) -> None:
        nonlocal connection
        connection = new_conn

    server_generator = _ws_server_internal(recv, set_conn, state)
    await anext(server_generator)

    async def urimeta() -> UriAndMetadata[None]:
        ipv4_laddr = state["ipv4_laddr"]
        assert ipv4_laddr
        return UriAndMetadata(uri="ws://%s:%d" % ipv4_laddr, metadata=None)

    yield (urimeta, recv, lambda: connection)

    connection = None

    try:
        await anext(server_generator)
    except StopAsyncIteration:
        pass
