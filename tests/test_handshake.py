from time import time

import pytest
import websockets
from websockets.exceptions import ConnectionClosedOK
from websockets.server import serve

from replit_river.server import Server
from replit_river.transport_options import TransportOptions


@pytest.fixture
def transport_options() -> TransportOptions:
    return TransportOptions(handshake_timeout_ms=200)


@pytest.mark.asyncio
@pytest.mark.parametrize("handlers", [{}])
async def test_handshake_timeout(server: Server) -> None:
    async with serve(server.serve, "127.0.0.1") as binding:
        sockets = list(binding.sockets)
        assert len(sockets) == 1, "Too many sockets!"
        socket = sockets[0]
        start = time()
        ws = await websockets.connect("ws://%s:%d" % socket.getsockname())
        with pytest.raises(ConnectionClosedOK):
            await ws.recv()
        diff = time() - start
        # we should wait at least 200ms but not for too long
        assert diff > 0.2 and diff < 1.0
