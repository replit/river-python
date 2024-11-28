import asyncio
import logging
from typing import Any, AsyncGenerator, Literal, Mapping

import nanoid
import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from websockets.server import serve

from replit_river.client import Client
from replit_river.client_transport import UriAndMetadata
from replit_river.error_schema import RiverError
from replit_river.rpc import (
    GenericRpcHandler,
    TransportMessage,
)
from replit_river.server import Server
from replit_river.transport_options import TransportOptions
from tests.river_fixtures.logging import NoErrors

# Modular fixtures
pytest_plugins = ["tests.river_fixtures.logging"]

HandlerMapping = Mapping[tuple[str, str], tuple[str, GenericRpcHandler]]


def transport_message(
    seq: int = 0,
    ack: int = 0,
    streamId: str = "test_stream",
    from_: str = "client",
    to: str = "server",
    control_flag: int = 0,
    payload: Any = {},
) -> TransportMessage:
    return TransportMessage(
        id=str(nanoid.generate()),
        from_=from_,  # type: ignore
        to=to,
        streamId=streamId,
        seq=seq,
        ack=ack,
        payload=payload,
        controlFlags=control_flag,
    )


def serialize_request(request: str) -> dict:
    return {"data": request}


def deserialize_request(request: dict) -> str:
    return request.get("data") or ""


def serialize_response(response: str) -> dict:
    return {"data": response}


def deserialize_response(response: dict) -> str:
    return response.get("data") or ""


def deserialize_error(response: dict) -> RiverError:
    return RiverError.model_validate(response)


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
async def client(
    server: Server,
    transport_options: TransportOptions,
    no_logging_error: NoErrors,
) -> AsyncGenerator[Client, None]:
    async def websocket_uri_factory() -> UriAndMetadata[None]:
        return {
            "uri": "ws://localhost:8765",
            "metadata": None,
        }

    try:
        async with serve(server.serve, "localhost", 8765):
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
        await asyncio.sleep(1)
        logging.debug("Start closing test server")
        await server.close()
        # Server should close normally
        no_logging_error()


@pytest.fixture(scope="session")
def span_exporter() -> InMemorySpanExporter:
    exporter = InMemorySpanExporter()
    processor = SimpleSpanProcessor(exporter)
    provider = TracerProvider()
    provider.add_span_processor(processor)
    trace.set_tracer_provider(provider)
    return exporter


@pytest.fixture(autouse=True)
def reset_span_exporter(span_exporter: InMemorySpanExporter) -> None:
    span_exporter.clear()
