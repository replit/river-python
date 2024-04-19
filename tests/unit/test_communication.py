import asyncio
from typing import AsyncGenerator

import pytest
import websockets
from websockets.server import serve

from replit_river.client import Client
from replit_river.error_schema import RiverError, RiverException
from replit_river.server import Server
from replit_river.transport_options import TransportOptions
from tests.conftest import deserialize_error, deserialize_response, serialize_request


@pytest.mark.asyncio
async def test_rpc_method(client: Client) -> None:
    response = await client.send_rpc(
        "test_service",
        "rpc_method",
        "Alice",
        serialize_request,
        deserialize_response,
        deserialize_error,
    )  # type: ignore
    assert response == "Hello, Alice!"


@pytest.mark.asyncio
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
        deserialize_response,
    )  # type: ignore
    assert response == "Uploaded: Initial Data, Data 1, Data 2, Data 3"


@pytest.mark.asyncio
async def test_subscription_method(client: Client) -> None:
    async for response in await client.send_subscription(
        "test_service",
        "subscription_method",
        "Bob",
        serialize_request,
        deserialize_response,
        deserialize_error,
    ):
        assert "Subscription message" in response


@pytest.mark.asyncio
async def test_stream_method(client: Client) -> None:
    async def stream_data() -> AsyncGenerator[str, None]:
        yield "Stream 1"
        yield "Stream 2"
        yield "Stream 3"

    responses = []
    async for response in await client.send_stream(
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
    stream_task = await client.send_stream(
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


@pytest.mark.asyncio
async def test_close_old_websocket_rpc(
    server: Server, transport_options: TransportOptions
) -> None:
    try:
        async with serve(server.serve, "localhost", 8765):
            async with serve(server.serve, "localhost", 8766):
                async with websockets.connect("ws://localhost:8765") as websocket1:
                    async with websockets.connect("ws://localhost:8766") as websocket2:
                        websockets_list = [websocket1, websocket2]
                        clients: list[Client] = []
                        num_clients = 2

                        async def create_clients() -> None:
                            for i in range(num_clients):
                                client = Client(
                                    websockets_list[i],
                                    client_id=f"client-{i}",
                                    server_id="test_server",
                                    transport_options=transport_options,
                                )
                                clients.append(client)

                        await create_clients()
                        with pytest.raises(RiverException):
                            await clients[0].send_rpc(
                                "test_service",
                                "rpc_method",
                                clients[0]._client_id,
                                serialize_request,
                                deserialize_response,
                                deserialize_error,
                            )
                        response = await clients[1].send_rpc(
                            "test_service",
                            "rpc_method",
                            clients[1]._client_id,
                            serialize_request,
                            deserialize_response,
                            deserialize_error,
                        )
                        assert response == f"Hello, client-{1}!"
    finally:
        await server.close()
        for client in clients:
            await client.close()
