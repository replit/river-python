import asyncio
import importlib
import os
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
from tests.conftest import HandlerMapping, deserialize_request, serialize_response
from tests.v1.common_handlers import basic_rpc_method


@pytest.fixture(scope="session", autouse=True)
def generate_rpc_client() -> None:
    shutil.rmtree("tests/v1/codegen/rpc/generated", ignore_errors=True)
    os.makedirs("tests/v1/codegen/rpc/generated")

    def file_opener(path: Path) -> TextIO:
        return open(path, "w")

    schema_to_river_client_codegen(
        read_schema=lambda: open("tests/v1/codegen/rpc/schema.json"),
        target_path="tests/v1/codegen/rpc/generated",
        client_name="RpcClient",
        typed_dict_inputs=True,
        file_opener=file_opener,
        method_filter=None,
        protocol_version="v1.1",
    )


@pytest.fixture(scope="session", autouse=True)
def reload_rpc_import(generate_rpc_client: None) -> None:
    import tests.v1.codegen.rpc.generated

    importlib.reload(tests.v1.codegen.rpc.generated)


@pytest.mark.asyncio
@pytest.mark.parametrize("handlers", [{**basic_rpc_method}])
async def test_basic_rpc(client: Client) -> None:
    from tests.v1.codegen.rpc.generated import RpcClient

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
    from tests.v1.codegen.rpc.generated import RpcClient

    with pytest.raises(RiverException):
        await RpcClient(client).test_service.rpc_method(
            {
                "data": "feep",
            },
            timedelta(milliseconds=200),
        )
