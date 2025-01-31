import asyncio
import importlib
import shutil
from datetime import timedelta
from pathlib import Path
from typing import TextIO

import grpc
import grpc.aio
import pytest

from replit_river.client import Client
from replit_river.codegen.client import schema_to_river_client_codegen
from replit_river.error_schema import RiverException
from replit_river.rpc import rpc_method_handler
from tests.common_handlers import basic_rpc_method
from tests.conftest import HandlerMapping, deserialize_request, serialize_response


@pytest.fixture(scope="session", autouse=True)
def generate_rpc_client() -> None:
    import tests.codegen.rpc.generated

    shutil.rmtree("tests/codegen/rpc/generated")

    def file_opener(path: Path) -> TextIO:
        return open(path, "w")

    schema_to_river_client_codegen(
        lambda: open("tests/codegen/rpc/schema.json"),
        "tests/codegen/rpc/generated",
        "RpcClient",
        True,
        file_opener,
    )
    importlib.reload(tests.codegen.rpc.generated)


@pytest.mark.asyncio
@pytest.mark.parametrize("handlers", [{**basic_rpc_method}])
async def test_basic_rpc(client: Client) -> None:
    from tests.codegen.rpc.generated import RpcClient

    res = await RpcClient(client).test_service.rpc_method(
        {
            "data": "feep",
        },
        timedelta(seconds=5),
    )
    assert res.data == "Hello, feep!", f"Expected 'Hello, feep!' received {res.data}"


async def rpc_timeout_handler(request: str, context: grpc.aio.ServicerContext) -> str:
    await asyncio.sleep(1)
    return f"Hello, {request}!"


rpc_timeout_method: HandlerMapping = {
    ("test_service", "rpc_method"): (
        "rpc",
        rpc_method_handler(
            rpc_timeout_handler, deserialize_request, serialize_response
        ),
    )
}


@pytest.mark.asyncio
@pytest.mark.parametrize("handlers", [{**rpc_timeout_method}])
async def test_rpc_timeout(client: Client) -> None:
    from tests.codegen.rpc.generated import RpcClient

    with pytest.raises(RiverException):
        await RpcClient(client).test_service.rpc_method(
            {
                "data": "feep",
            },
            timedelta(milliseconds=200),
        )
