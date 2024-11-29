import shutil
from datetime import timedelta

import pytest

import importlib
from replit_river.client import Client
from replit_river.codegen.client import schema_to_river_client_codegen
from tests.common_handlers import basic_rpc_method


@pytest.fixture(scope="session", autouse=True)
def generate_rpc_client() -> None:
    import tests.codegen.rpc.generated

    shutil.rmtree("tests/codegen/rpc/generated")
    schema_to_river_client_codegen(
        "tests/codegen/rpc/schema.json",
        "tests/codegen/rpc/generated",
        "RpcClient",
        True,
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
