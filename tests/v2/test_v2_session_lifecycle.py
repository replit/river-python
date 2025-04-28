import asyncio
from typing import AsyncIterator, Awaitable, Callable, TypeAlias, TypedDict

import pytest
from websockets import ConnectionClosedOK
from websockets.asyncio.server import ServerConnection, serve
from websockets.typing import Data

from replit_river.messages import parse_transport_msg
from replit_river.rate_limiter import RateLimiter
from replit_river.rpc import TransportMessage
from replit_river.transport_options import TransportOptions, UriAndMetadata
from replit_river.v2.session import Session


class _PermissiveRateLimiter(RateLimiter):
    def start_restoring_budget(self, user: str) -> None:
        pass

    def get_backoff_ms(self, user: str) -> float:
        return 0

    def has_budget(self, user: str) -> bool:
        return True

    def consume_budget(self, user: str) -> None:
        pass


WsServerFixture: TypeAlias = tuple[
    Callable[[], Awaitable[UriAndMetadata[None]]],
    asyncio.Queue[bytes],
    Callable[[], ServerConnection | None],
]


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
        serve_forever.cancel()


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

    try:
        await anext(server_generator)
    except StopAsyncIteration:
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


async def test_reconnect(ws_server: WsServerFixture) -> None:
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
