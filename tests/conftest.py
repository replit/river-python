import asyncio
import logging
from collections.abc import AsyncIterator
from typing import Any, AsyncGenerator, Iterator, Literal, Mapping

import grpc.aio
import nanoid  # type: ignore
import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from websockets.server import serve

from replit_river.client import Client
from replit_river.client_transport import UriAndMetadata
from replit_river.error_schema import RiverError, RiverException
from replit_river.rpc import (
    GenericRpcHandler,
    TransportMessage,
    rpc_method_handler,
    stream_method_handler,
    subscription_method_handler,
    upload_method_handler,
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


# RPC method handlers for testing
async def rpc_handler(request: str, context: grpc.aio.ServicerContext) -> str:
    return f"Hello, {request}!"


async def subscription_handler(
    request: str, context: grpc.aio.ServicerContext
) -> AsyncGenerator[str, None]:
    for i in range(5):
        yield f"Subscription message {i} for {request}"


async def upload_handler(
    request: Iterator[str] | AsyncIterator[str], context: Any
) -> str:
    uploaded_data = []
    if isinstance(request, AsyncIterator):
        async for data in request:
            uploaded_data.append(data)
    else:
        for data in request:
            uploaded_data.append(data)
    return f"Uploaded: {', '.join(uploaded_data)}"


async def stream_handler(
    request: Iterator[str] | AsyncIterator[str],
    context: grpc.aio.ServicerContext,
) -> AsyncGenerator[str, None]:
    if isinstance(request, AsyncIterator):
        async for data in request:
            yield f"Stream response for {data}"
    else:
        for data in request:
            yield f"Stream response for {data}"


async def stream_error_handler(
    request: Iterator[str] | AsyncIterator[str],
    context: grpc.aio.ServicerContext,
) -> AsyncGenerator[str, None]:
    raise RiverException("INJECTED_ERROR", "test error")
    yield "test"  # appease the type checker


common_handlers: HandlerMapping = {
    ("test_service", "rpc_method"): (
        "rpc",
        rpc_method_handler(rpc_handler, deserialize_request, serialize_response),
    ),
    ("test_service", "subscription_method"): (
        "subscription",
        subscription_method_handler(
            subscription_handler, deserialize_request, serialize_response
        ),
    ),
    ("test_service", "upload_method"): (
        "upload",
        upload_method_handler(upload_handler, deserialize_request, serialize_response),
    ),
    ("test_service", "stream_method"): (
        "stream",
        stream_method_handler(stream_handler, deserialize_request, serialize_response),
    ),
    ("test_service", "stream_method_error"): (
        "stream",
        stream_method_handler(
            stream_error_handler, deserialize_request, serialize_response
        ),
    ),
}


@pytest.fixture
def transport_options() -> TransportOptions:
    return TransportOptions()


@pytest.fixture
def server(transport_options: TransportOptions) -> Server:
    server = Server(server_id="test_server", transport_options=transport_options)
    server.add_rpc_handlers(common_handlers)
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
