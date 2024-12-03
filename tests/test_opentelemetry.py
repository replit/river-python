import contextlib
from datetime import timedelta
from typing import AsyncGenerator, AsyncIterator, Iterator

import grpc
import grpc.aio
import pytest
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode

from replit_river.client import Client
from replit_river.error_schema import RiverError, RiverException
from replit_river.rpc import stream_method_handler
from tests.common_handlers import (
    basic_rpc_method,
    basic_stream,
    basic_subscription,
    basic_upload,
)
from tests.conftest import (
    HandlerMapping,
    deserialize_error,
    deserialize_request,
    deserialize_response,
    serialize_request,
    serialize_response,
)
from tests.river_fixtures.logging import NoErrors


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
