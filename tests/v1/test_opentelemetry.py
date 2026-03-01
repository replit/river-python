import contextlib
import logging
from datetime import timedelta
from typing import AsyncGenerator, AsyncIterator, Iterator, Literal

import grpc
import grpc.aio
import pytest
from opentelemetry import baggage, context, propagate, trace
from opentelemetry.baggage.propagation import W3CBaggagePropagator
from opentelemetry.propagators.composite import CompositePropagator
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from websockets.server import serve

from replit_river.client import Client
from replit_river.client_transport import UriAndMetadata
from replit_river.error_schema import RiverError, RiverException
from replit_river.rpc import rpc_method_handler, stream_method_handler
from replit_river.server import Server
from replit_river.transport_options import TransportOptions
from tests.conftest import (
    HandlerMapping,
    deserialize_error,
    deserialize_request,
    deserialize_response,
    serialize_request,
    serialize_response,
)
from tests.v1.common_handlers import (
    basic_rpc_method,
    basic_stream,
    basic_subscription,
    basic_upload,
)
from tests.v1.river_fixtures.logging import NoErrors


@pytest.mark.asyncio
@pytest.mark.parametrize("handlers", [{**basic_rpc_method}])
async def test_rpc_method_span(
    client: Client, span_exporter: InMemorySpanExporter
) -> None:
    response = await client.send_rpc(
        "test_service",
        "rpc_method",
        "Alice",
        serialize_request,
        deserialize_response,
        deserialize_error,
        timedelta(seconds=20),
    )
    assert response == "Hello, Alice!"
    spans = span_exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "river.client.rpc.test_service.rpc_method"


@pytest.mark.asyncio
@pytest.mark.parametrize("handlers", [{**basic_upload}])
async def test_upload_method_span(
    client: Client, span_exporter: InMemorySpanExporter
) -> None:
    async def upload_data() -> AsyncGenerator[str, None]:
        yield "Data 1"
        yield "Data 2"
        yield "Data 3"

    response = await client.send_upload(
        "test_service",
        "upload_method",
        "Initial Data",
        upload_data(),
        serialize_request,
        serialize_request,
        deserialize_response,
        deserialize_error,
    )
    assert response == "Uploaded: Initial Data, Data 1, Data 2, Data 3"
    spans = span_exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "river.client.upload.test_service.upload_method"


@pytest.mark.asyncio
@pytest.mark.parametrize("handlers", [{**basic_subscription}])
async def test_subscription_method_span(
    client: Client, span_exporter: InMemorySpanExporter
) -> None:
    async for response in client.send_subscription(
        "test_service",
        "subscription_method",
        "Bob",
        serialize_request,
        deserialize_response,
        deserialize_error,
    ):
        assert isinstance(response, str)
        assert "Subscription message" in response

    spans = span_exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "river.client.subscription.test_service.subscription_method"


@pytest.mark.asyncio
@pytest.mark.parametrize("handlers", [{**basic_stream}])
async def test_stream_method_span(
    client: Client, span_exporter: InMemorySpanExporter
) -> None:
    async def stream_data() -> AsyncGenerator[str, None]:
        yield "Stream 1"
        yield "Stream 2"
        yield "Stream 3"

    responses = []
    async for response in client.send_stream(
        "test_service",
        "stream_method",
        "Initial Stream Data",
        stream_data(),
        serialize_request,
        serialize_request,
        deserialize_response,
        deserialize_error,
    ):
        responses.append(response)

    assert responses == [
        "Stream response for Initial Stream Data",
        "Stream response for Stream 1",
        "Stream response for Stream 2",
        "Stream response for Stream 3",
    ]

    spans = span_exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "river.client.stream.test_service.stream_method"


async def stream_error_handler(
    request: Iterator[str] | AsyncIterator[str],
    context: grpc.aio.ServicerContext,
) -> AsyncGenerator[str, None]:
    raise RiverException("INJECTED_ERROR", "test error")
    yield "test"  # appease the type checker


stream_error_method_handlers: HandlerMapping = {
    ("test_service", "stream_method_error"): (
        "stream",
        stream_method_handler(
            stream_error_handler, deserialize_request, serialize_response
        ),
    )
}


@pytest.mark.asyncio
@pytest.mark.parametrize("handlers", [{**stream_error_method_handlers}])
async def test_stream_error_method_span(
    client: Client,
    span_exporter: InMemorySpanExporter,
    no_logging_error: NoErrors,
) -> None:
    # We are explicitly testing errors.
    no_logging_error.allow_errors()

    async def stream_data() -> AsyncGenerator[str, None]:
        yield "Stream 1"
        yield "Stream 2"
        yield "Stream 3"

    responses = []
    async for response in client.send_stream(
        "test_service",
        "stream_method_error",
        "Initial Stream Data",
        stream_data(),
        serialize_request,
        serialize_request,
        deserialize_response,
        deserialize_error,
    ):
        responses.append(response)

    assert len(responses) == 1
    assert isinstance(responses[0], RiverError)

    spans = span_exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "river.client.stream.test_service.stream_method_error"
    assert spans[0].status.status_code == StatusCode.ERROR


@pytest.mark.asyncio
@pytest.mark.parametrize("handlers", [{**basic_stream}])
async def test_stream_method_span_generator_exit_not_recorded(
    client: Client, span_exporter: InMemorySpanExporter
) -> None:
    async def stream_data() -> AsyncGenerator[str, None]:
        yield "Stream 1"
        yield "Stream 2"
        yield "Stream 3"

    responses = []
    stream = client.send_stream(
        "test_service",
        "stream_method",
        "Initial Stream Data",
        stream_data(),
        serialize_request,
        serialize_request,
        deserialize_response,
        deserialize_error,
    )
    async with contextlib.aclosing(stream) as generator:
        async for response in generator:
            responses.append(response)
            break

    assert responses == [
        "Stream response for Initial Stream Data",
    ]

    spans = span_exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "river.client.stream.test_service.stream_method"
    assert spans[0].status.status_code == StatusCode.OK


# ===== OTel context propagation via WebSocket HTTP upgrade headers =====


# A handler that reads OTel baggage from the ambient context and returns it.
async def baggage_echo_handler(request: str, ctx: grpc.aio.ServicerContext) -> str:
    all_baggage = baggage.get_all()
    # Return baggage as a comma-separated "key=value" string
    return ",".join(f"{k}={v}" for k, v in sorted(all_baggage.items()))


baggage_echo_handlers: HandlerMapping = {
    ("test_service", "baggage_echo"): (
        "rpc",
        rpc_method_handler(
            baggage_echo_handler, deserialize_request, serialize_response
        ),
    )
}


@pytest.fixture
def _enable_baggage_propagator():
    """Temporarily install a composite propagator that includes both
    W3C TraceContext and W3C Baggage propagation so that
    ``propagate.inject()`` / ``propagate.extract()`` handle the
    ``baggage`` HTTP header."""
    previous = propagate.get_global_textmap()
    propagate.set_global_textmap(
        CompositePropagator(
            [
                TraceContextTextMapPropagator(),
                W3CBaggagePropagator(),
            ]
        )
    )
    yield
    propagate.set_global_textmap(previous)


@pytest.mark.asyncio
@pytest.mark.parametrize("handlers", [{**baggage_echo_handlers}])
@pytest.mark.usefixtures("_enable_baggage_propagator")
async def test_baggage_propagated_via_ws_headers(
    no_logging_error: NoErrors,
    server: Server,
    transport_options: TransportOptions,
) -> None:
    """Verify that OTel baggage set on the client side is propagated to the
    server via the WebSocket HTTP upgrade request headers."""

    # Set baggage in the ambient OTel context *before* the client connects,
    # so that ``propagate.inject()`` (called inside ``websockets.connect()``)
    # includes the ``baggage`` header.
    ctx = baggage.set_baggage("test-key", "test-value")
    ctx = baggage.set_baggage("another-key", "another-value", context=ctx)
    token = context.attach(ctx)

    binding = None
    try:
        binding = await serve(server.serve, "127.0.0.1")
        sockets = list(binding.sockets)
        assert len(sockets) == 1
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
            response = await client.send_rpc(
                "test_service",
                "baggage_echo",
                "ignored",
                serialize_request,
                deserialize_response,
                deserialize_error,
                timedelta(seconds=20),
            )
            # The handler returns sorted "key=value" pairs
            assert response == "another-key=another-value,test-key=test-value"
        finally:
            logging.debug("Start closing test client")
            await client.close()
    finally:
        context.detach(token)
        logging.debug("Start closing test server")
        if binding:
            binding.close()
        await server.close()
        if binding:
            await binding.wait_closed()


@pytest.mark.asyncio
@pytest.mark.parametrize("handlers", [{**baggage_echo_handlers}])
@pytest.mark.usefixtures("_enable_baggage_propagator")
async def test_no_baggage_when_none_set(
    no_logging_error: NoErrors,
    server: Server,
    transport_options: TransportOptions,
) -> None:
    """Verify that when no baggage is set, the server sees empty baggage."""

    binding = None
    try:
        binding = await serve(server.serve, "127.0.0.1")
        sockets = list(binding.sockets)
        assert len(sockets) == 1
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
            response = await client.send_rpc(
                "test_service",
                "baggage_echo",
                "ignored",
                serialize_request,
                deserialize_response,
                deserialize_error,
                timedelta(seconds=20),
            )
            assert response == ""
        finally:
            logging.debug("Start closing test client")
            await client.close()
    finally:
        logging.debug("Start closing test server")
        if binding:
            binding.close()
        await server.close()
        if binding:
            await binding.wait_closed()


@pytest.mark.asyncio
@pytest.mark.parametrize("handlers", [{**baggage_echo_handlers}])
@pytest.mark.usefixtures("_enable_baggage_propagator")
async def test_traceparent_propagated_via_ws_headers(
    no_logging_error: NoErrors,
    server: Server,
    transport_options: TransportOptions,
    span_exporter: InMemorySpanExporter,
) -> None:
    """Verify that when a span is active on the client, the traceparent
    header is sent on the WS upgrade and the server-side context inherits
    the trace."""
    tracer = trace.get_tracer(__name__)

    with tracer.start_as_current_span("client-operation"):
        # Also set some baggage
        ctx = baggage.set_baggage("trace-test", "yes")
        token = context.attach(ctx)

        binding = None
        try:
            binding = await serve(server.serve, "127.0.0.1")
            sockets = list(binding.sockets)
            assert len(sockets) == 1
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
                response = await client.send_rpc(
                    "test_service",
                    "baggage_echo",
                    "ignored",
                    serialize_request,
                    deserialize_response,
                    deserialize_error,
                    timedelta(seconds=20),
                )
                # Verify baggage was propagated
                assert response == "trace-test=yes"
            finally:
                logging.debug("Start closing test client")
                await client.close()
        finally:
            context.detach(token)
            logging.debug("Start closing test server")
            if binding:
                binding.close()
            await server.close()
            if binding:
                await binding.wait_closed()
