import logging
from typing import AsyncGenerator

import pytest
from websockets.server import serve

from replit_river.client import Client
from replit_river.error_schema import RiverError
from replit_river.rpc import (
    GrpcContext,
    rpc_method_handler,
    stream_method_handler,
    subscription_method_handler,
    upload_method_handler,
)
from replit_river.server import Server
from replit_river.transport_options import TransportOptions


def serialize_request(request: str) -> dict:
    return {"data": request}


def deserialize_request(request: dict) -> str:
    return request["data"] or ""


def serialize_response(response: str) -> dict:
    return {"data": response}


def deserialize_response(response: dict) -> str:
    return response["data"] or ""


def deserialize_error(response: dict) -> RiverError:
    return RiverError.model_validate(response)


# RPC method handlers for testing
async def rpc_handler(request: str, context: GrpcContext) -> str:
    return f"Hello, {request}!"


async def subscription_handler(
    request: str, context: GrpcContext
) -> AsyncGenerator[str, None]:
    for i in range(5):
        yield f"Subscription message {i} for {request}"


async def upload_handler(
    request: AsyncGenerator[str, None], context: GrpcContext
) -> str:
    uploaded_data = []
    async for data in request:
        uploaded_data.append(data)
    return f"Uploaded: {', '.join(uploaded_data)}"


async def stream_handler(
    request: AsyncGenerator[str, None], context: GrpcContext
) -> AsyncGenerator[str, None]:
    async for data in request:
        yield f"Stream response for {data}"


@pytest.fixture
def transport_options() -> TransportOptions:
    return TransportOptions()


@pytest.fixture
def server(transport_options: TransportOptions) -> Server:
    server = Server(server_id="test_server", transport_options=transport_options)
    server.add_rpc_handlers(
        {
            ("test_service", "rpc_method"): (
                "rpc",
                rpc_method_handler(
                    rpc_handler, deserialize_request, serialize_response
                ),
            ),
            ("test_service", "subscription_method"): (
                "subscription",
                subscription_method_handler(
                    subscription_handler, deserialize_request, serialize_response
                ),
            ),
            ("test_service", "upload_method"): (
                "upload",
                upload_method_handler(
                    upload_handler, deserialize_request, serialize_response
                ),
            ),
            ("test_service", "stream_method"): (
                "stream",
                stream_method_handler(
                    stream_handler, deserialize_request, serialize_response
                ),
            ),
        }
    )
    return server


@pytest.fixture
async def client(
    server: Server, transport_options: TransportOptions
) -> AsyncGenerator[Client, None]:
    try:
        async with serve(server.serve, "localhost", 8765):
            client = Client(
                "ws://localhost:8765",
                client_id="test_client",
                server_id="test_server",
                transport_options=transport_options,
            )
            try:
                yield client
            finally:
                logging.debug(f"Start closing test client : {'test_client'}")
                await client.close()
    finally:
        logging.debug("Start closing test server")
        await server.close()
