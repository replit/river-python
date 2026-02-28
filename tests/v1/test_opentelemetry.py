import contextlib
from datetime import timedelta
from typing import AsyncGenerator, AsyncIterator, Iterator

import grpc
import grpc.aio
import pytest
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind, StatusCode

from replit_river.client import Client
from replit_river.error_schema import RiverError, RiverException
from replit_river.rpc import stream_method_handler
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
    assert len(spans) == 2
    client_spans = [s for s in spans if "client" in s.name]
    server_spans = [s for s in spans if "server" in s.name]
    assert len(client_spans) == 1
    assert len(server_spans) == 1
    assert client_spans[0].name == "river.client.rpc.test_service.rpc_method"
    assert server_spans[0].name == "river.server.rpc.test_service.rpc_method"


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
    assert len(spans) == 2
    client_spans = [s for s in spans if "client" in s.name]
    server_spans = [s for s in spans if "server" in s.name]
    assert len(client_spans) == 1
    assert len(server_spans) == 1
    assert client_spans[0].name == "river.client.upload.test_service.upload_method"
    assert (
        server_spans[0].name
        == "river.server.upload-stream.test_service.upload_method"
    )


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
    assert len(spans) == 2
    client_spans = [s for s in spans if "client" in s.name]
    server_spans = [s for s in spans if "server" in s.name]
    assert len(client_spans) == 1
    assert len(server_spans) == 1
    assert (
        client_spans[0].name
        == "river.client.subscription.test_service.subscription_method"
    )
    assert (
        server_spans[0].name
        == "river.server.subscription-stream.test_service.subscription_method"
    )


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
    assert len(spans) == 2
    client_spans = [s for s in spans if "client" in s.name]
    server_spans = [s for s in spans if "server" in s.name]
    assert len(client_spans) == 1
    assert len(server_spans) == 1
    assert client_spans[0].name == "river.client.stream.test_service.stream_method"
    assert server_spans[0].name == "river.server.stream.test_service.stream_method"


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
    client_spans = [s for s in spans if "client" in s.name]
    server_spans = [s for s in spans if "server" in s.name]
    assert len(client_spans) == 1
    assert len(server_spans) == 1
    assert (
        client_spans[0].name
        == "river.client.stream.test_service.stream_method_error"
    )
    assert client_spans[0].status.status_code == StatusCode.ERROR
    assert (
        server_spans[0].name
        == "river.server.stream.test_service.stream_method_error"
    )
    assert server_spans[0].status.status_code == StatusCode.OK


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
    client_spans = [s for s in spans if "client" in s.name]
    assert len(client_spans) == 1
    assert client_spans[0].name == "river.client.stream.test_service.stream_method"
    assert client_spans[0].status.status_code == StatusCode.OK


# ===== Trace propagation tests =====


@pytest.mark.asyncio
@pytest.mark.parametrize("handlers", [{**basic_rpc_method}])
async def test_rpc_trace_propagation(
    client: Client, span_exporter: InMemorySpanExporter
) -> None:
    """Test that the server span is a child of the client span (same trace)."""
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
    client_spans = [s for s in spans if "client" in s.name]
    server_spans = [s for s in spans if "server" in s.name]
    assert len(client_spans) == 1
    assert len(server_spans) == 1

    client_span = client_spans[0]
    server_span = server_spans[0]

    # Both spans should share the same trace ID
    assert client_span.context.trace_id == server_span.context.trace_id

    # Server span should be a child of the client span
    assert server_span.parent is not None
    assert server_span.parent.span_id == client_span.context.span_id

    # Verify span kinds
    assert client_span.kind == SpanKind.CLIENT
    assert server_span.kind == SpanKind.SERVER

    # Both should be OK
    assert client_span.status.status_code == StatusCode.OK
    assert server_span.status.status_code == StatusCode.OK


@pytest.mark.asyncio
@pytest.mark.parametrize("handlers", [{**basic_subscription}])
async def test_subscription_trace_propagation(
    client: Client, span_exporter: InMemorySpanExporter
) -> None:
    """Test that trace context propagates for subscriptions."""
    async for response in client.send_subscription(
        "test_service",
        "subscription_method",
        "Bob",
        serialize_request,
        deserialize_response,
        deserialize_error,
    ):
        pass

    spans = span_exporter.get_finished_spans()
    client_spans = [s for s in spans if "client" in s.name]
    server_spans = [s for s in spans if "server" in s.name]
    assert len(client_spans) == 1
    assert len(server_spans) == 1

    client_span = client_spans[0]
    server_span = server_spans[0]

    assert client_span.context.trace_id == server_span.context.trace_id
    assert server_span.parent is not None
    assert server_span.parent.span_id == client_span.context.span_id


@pytest.mark.asyncio
@pytest.mark.parametrize("handlers", [{**basic_upload}])
async def test_upload_trace_propagation(
    client: Client, span_exporter: InMemorySpanExporter
) -> None:
    """Test that trace context propagates for uploads."""

    async def upload_data() -> AsyncGenerator[str, None]:
        yield "Data 1"
        yield "Data 2"

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
    assert response == "Uploaded: Initial Data, Data 1, Data 2"

    spans = span_exporter.get_finished_spans()
    client_spans = [s for s in spans if "client" in s.name]
    server_spans = [s for s in spans if "server" in s.name]
    assert len(client_spans) == 1
    assert len(server_spans) == 1

    client_span = client_spans[0]
    server_span = server_spans[0]

    assert client_span.context.trace_id == server_span.context.trace_id
    assert server_span.parent is not None
    assert server_span.parent.span_id == client_span.context.span_id


@pytest.mark.asyncio
@pytest.mark.parametrize("handlers", [{**basic_stream}])
async def test_stream_trace_propagation(
    client: Client, span_exporter: InMemorySpanExporter
) -> None:
    """Test that trace context propagates for bidirectional streams."""

    async def stream_data() -> AsyncGenerator[str, None]:
        yield "Stream 1"
        yield "Stream 2"

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

    assert len(responses) == 3  # Initial + 2 stream items

    spans = span_exporter.get_finished_spans()
    client_spans = [s for s in spans if "client" in s.name]
    server_spans = [s for s in spans if "server" in s.name]
    assert len(client_spans) == 1
    assert len(server_spans) == 1

    client_span = client_spans[0]
    server_span = server_spans[0]

    assert client_span.context.trace_id == server_span.context.trace_id
    assert server_span.parent is not None
    assert server_span.parent.span_id == client_span.context.span_id


@pytest.mark.asyncio
@pytest.mark.parametrize("handlers", [{**basic_rpc_method}])
async def test_server_span_has_attributes(
    client: Client, span_exporter: InMemorySpanExporter
) -> None:
    """Test that server spans have the expected attributes."""
    await client.send_rpc(
        "test_service",
        "rpc_method",
        "Alice",
        serialize_request,
        deserialize_response,
        deserialize_error,
        timedelta(seconds=20),
    )

    spans = span_exporter.get_finished_spans()
    server_spans = [s for s in spans if "server" in s.name]
    assert len(server_spans) == 1
    server_span = server_spans[0]

    assert server_span.attributes is not None
    assert server_span.attributes.get("river.service_name") == "test_service"
    assert server_span.attributes.get("river.procedure_name") == "rpc_method"
    assert server_span.attributes.get("river.method_type") == "rpc"
    assert server_span.attributes.get("river.client_id") == "test_client"
    assert server_span.attributes.get("river.stream_id") is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("handlers", [{**basic_rpc_method}])
async def test_multiple_rpcs_have_independent_traces(
    client: Client, span_exporter: InMemorySpanExporter
) -> None:
    """Test that independent RPCs create independent traces."""
    await client.send_rpc(
        "test_service",
        "rpc_method",
        "Alice",
        serialize_request,
        deserialize_response,
        deserialize_error,
        timedelta(seconds=20),
    )
    await client.send_rpc(
        "test_service",
        "rpc_method",
        "Bob",
        serialize_request,
        deserialize_response,
        deserialize_error,
        timedelta(seconds=20),
    )

    spans = span_exporter.get_finished_spans()
    assert len(spans) == 4  # 2 client + 2 server

    client_spans = [s for s in spans if "client" in s.name]
    server_spans = [s for s in spans if "server" in s.name]
    assert len(client_spans) == 2
    assert len(server_spans) == 2

    # Each RPC should have its own trace ID
    trace_id_1 = client_spans[0].context.trace_id
    trace_id_2 = client_spans[1].context.trace_id
    assert trace_id_1 != trace_id_2

    # Each server span should have matching trace IDs with its corresponding client span
    for server_span in server_spans:
        assert server_span.parent is not None
        matching_client = [
            c
            for c in client_spans
            if c.context.span_id == server_span.parent.span_id
        ]
        assert len(matching_client) == 1
        assert server_span.context.trace_id == matching_client[0].context.trace_id
