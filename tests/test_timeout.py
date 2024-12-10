import asyncio
from datetime import timedelta

import grpc
import grpc.aio
import pytest

from replit_river.client import Client
from replit_river.error_schema import ERROR_CODE_CANCEL, RiverException
from tests.common_handlers import (
    rpc_method_handler,
)
from tests.conftest import (
    HandlerMapping,
    deserialize_error,
    deserialize_response,
    serialize_response,
)


async def rpc_slow_handler(duration: float, context: grpc.aio.ServicerContext) -> str:
    await asyncio.sleep(duration)
    return "I'm sleepy"


def serialize_request(request: float) -> dict:
    return {"data": request}


def deserialize_request(request: dict) -> float:
    return request.get("data") or 0


slow_rpc: HandlerMapping = {
    ("test_service", "rpc_method"): (
        "rpc",
        rpc_method_handler(rpc_slow_handler, deserialize_request, serialize_response),
    ),
}


@pytest.mark.asyncio
@pytest.mark.parametrize("handlers", [{**slow_rpc}])
async def test_no_timeout(client: Client) -> None:
    res = await client.send_rpc(
        "test_service",
        "rpc_method",
        0.01,
        serialize_request,
        deserialize_response,
        deserialize_error,
        timeout=timedelta(seconds=0.1),
    )
    assert res == "I'm sleepy"


@pytest.mark.asyncio
@pytest.mark.parametrize("handlers", [{**slow_rpc}])
async def test_timeout(client: Client) -> None:
    with pytest.raises(RiverException) as exc_info:
        await client.send_rpc(
            "test_service",
            "rpc_method",
            2.0,
            serialize_request,
            deserialize_response,
            deserialize_error,
            timeout=timedelta(seconds=0.01),
        )
    assert exc_info.value.code == ERROR_CODE_CANCEL
