import logging
from typing import AsyncGenerator, Literal

import pytest
from websockets.server import serve

from replit_river.client import Client
from replit_river.client_transport import UriAndMetadata
from replit_river.server import Server
from replit_river.transport_options import TransportOptions
from tests.conftest import HandlerMapping
from tests.v1.river_fixtures.logging import NoErrors  # noqa: E402


@pytest.fixture
def transport_options() -> TransportOptions:
    return TransportOptions()


@pytest.fixture
def server_handlers(handlers: HandlerMapping) -> HandlerMapping:
    return handlers


@pytest.fixture
def server(
    transport_options: TransportOptions, server_handlers: HandlerMapping
) -> Server:
    server = Server(server_id="test_server", transport_options=transport_options)
    server.add_rpc_handlers(server_handlers)
    return server


@pytest.fixture
async def erroringClient(
    server: Server,
    transport_options: TransportOptions,
    no_logging_error: NoErrors,
) -> AsyncGenerator[Client, None]:
    binding = None
    try:
        binding = await serve(server.serve, "127.0.0.1")
        sockets = list(binding.sockets)
        assert len(sockets) == 1, "Too many sockets!"
        socket = sockets[0]

        async def websocket_uri_factory() -> UriAndMetadata[None]:
            return {
                "uri": "ws://%s:%d" % socket.getsockname(),
                "metadata": None,
            }

        client: Client[Literal[None]] = Client[None](
            uri_and_metadata_factory=websocket_uri_factory,
            client_id="test_client",
            server_id="test_server",
            transport_options=transport_options,
        )
        try:
            yield client
        finally:
            logging.debug("Start closing test client : %s", "test_client")
            await client.close()

    finally:
        logging.debug("Start closing test server")
        if binding:
            binding.close()
        await server.close()
        if binding:
            await binding.wait_closed()


@pytest.fixture
async def client(
    erroringClient: Client,
    no_logging_error: NoErrors,
) -> AsyncGenerator[Client, None]:
    yield erroringClient
    # Server should close normally
    no_logging_error()
