import asyncio
from datetime import timedelta
from typing import AsyncGenerator

import pytest
from grpc.aio import grpc

from replit_river.client import Client
from replit_river.error_schema import RiverError
from replit_river.rpc import subscription_method_handler
from replit_river.transport_options import MAX_MESSAGE_BUFFER_SIZE
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


@pytest.mark.asyncio
@pytest.mark.parametrize("handlers", [{**basic_rpc_method}])
async def test_rpc_method(client: Client) -> None:
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


@pytest.mark.asyncio
@pytest.mark.parametrize("handlers", [{**basic_upload}])
async def test_upload_method(client: Client) -> None:
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


@pytest.mark.asyncio
@pytest.mark.parametrize("handlers", [{**basic_upload}])
async def test_upload_more_than_send_buffer_max(client: Client) -> None:
    iterations = MAX_MESSAGE_BUFFER_SIZE * 2

    async def upload_data() -> AsyncGenerator[str, None]:
        for _ in range(0, iterations):
            yield "Data"

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
    assert response == "Uploaded: Initial Data" + (", Data" * iterations)


@pytest.mark.asyncio
@pytest.mark.parametrize("handlers", [{**basic_upload}])
async def test_upload_empty(client: Client) -> None:
    async def upload_data(enabled: bool = False) -> AsyncGenerator[str, None]:
        if enabled:
            yield "unreachable"

    response = await client.send_upload(
        "test_service",
        "upload_method",
        None,
        upload_data(),
        None,
        serialize_request,
        deserialize_response,
        deserialize_error,
    )
    assert response == "Uploaded: "


@pytest.mark.asyncio
@pytest.mark.parametrize("handlers", [{**basic_subscription}])
async def test_subscription_method(client: Client) -> None:
    messages = []
    async for response in client.send_subscription(
        "test_service",
        "subscription_method",
        "Bob",
        serialize_request,
        deserialize_response,
        deserialize_error,
    ):
        assert isinstance(response, str)
        messages.append(response)
    assert messages == [f"Subscription message {i} for Bob" for i in range(5)]


@pytest.mark.asyncio
@pytest.mark.parametrize("handlers", [{**basic_stream}])
async def test_stream_method(client: Client) -> None:
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


@pytest.mark.asyncio
@pytest.mark.parametrize("handlers", [{**basic_stream}])
async def test_stream_empty(client: Client) -> None:
    async def stream_data(enabled: bool = False) -> AsyncGenerator[str, None]:
        if enabled:
            yield "unreachable"

    responses = []
    async for response in client.send_stream(
        "test_service",
        "stream_method",
        None,
        stream_data(),
        None,
        serialize_request,
        deserialize_response,
        deserialize_error,
    ):
        responses.append(response)

    assert responses == []


@pytest.mark.asyncio
@pytest.mark.parametrize("handlers", [{**basic_upload, **basic_stream}])
async def test_multiplexing(client: Client) -> None:
    async def upload_data() -> AsyncGenerator[str, None]:
        yield "Upload Data 1"
        yield "Upload Data 2"

    async def stream_data() -> AsyncGenerator[str, None]:
        yield "Stream Data 1"
        yield "Stream Data 2"

    upload_task = asyncio.create_task(
        client.send_upload(
            "test_service",
            "upload_method",
            "Initial Upload Data",
            upload_data(),
            serialize_request,
            serialize_request,
            deserialize_response,
            deserialize_error,
        )
    )
    stream_task = client.send_stream(
        "test_service",
        "stream_method",
        "Initial Stream Data",
        stream_data(),
        serialize_request,
        serialize_request,
        deserialize_response,
        deserialize_error,
    )

    upload_response: str = await upload_task
    assert (
        upload_response == "Uploaded: Initial Upload Data, Upload Data 1, Upload Data 2"
    )

    stream_responses: list[str | RiverError] = []
    async for response in stream_task:
        stream_responses.append(response)

    assert stream_responses == [
        "Stream response for Initial Stream Data",
        "Stream response for Stream Data 1",
        "Stream response for Stream Data 2",
    ]


async def flood_subscription_handler(
    request: str, context: grpc.aio.ServicerContext
) -> AsyncGenerator[str, None]:
    for i in range(1024):
        yield f"Subscription message {i} for {request}"


flood_subscription: HandlerMapping = {
    ("test_service", "flood_subscription_method"): (
        "subscription-stream",
        subscription_method_handler(
            flood_subscription_handler, deserialize_request, serialize_response
        ),
    ),
}


@pytest.mark.asyncio
@pytest.mark.parametrize("handlers", [{**basic_rpc_method, **flood_subscription}])
async def test_ignore_flood_subscription(client: Client) -> None:
    sub = client.send_subscription(
        "test_service",
        "flood_subscription_method",
        "Initial Subscription Data",
        serialize_request,
        deserialize_response,
        deserialize_error,
    )

    # read one entry to start the subscription
    await sub.__anext__()
    # close the subscription so we can signal that we're not
    # interested in the rest of the subscription.
    await sub.aclose()

    # ensure that subsequent RPCs still work
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
