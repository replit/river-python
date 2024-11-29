import shutil
from datetime import timedelta

import pytest

from replit_river.client import Client
from replit_river.codegen.client import schema_to_river_client_codegen
from tests.common_handlers import basic_rpc_method


@pytest.mark.asyncio
@pytest.mark.parametrize("handlers", [{**basic_rpc_method}])
async def test_basic_rpc(client: Client) -> None:
    shutil.rmtree("tests/codegen/rpc/generated")
    schema_to_river_client_codegen(
        "tests/codegen/rpc/schema.json",
        "tests/codegen/rpc/generated",
        "RpcClient",
        True,
    )
    from tests.codegen.rpc.generated import RpcClient

    res = await RpcClient(client).test_service.rpc_method(
        {
            "data": "feep",
        },
        timedelta(seconds=5),
    )
    assert res.data == "Hello, feep!", f"Expected 'Hello, feep!' received {res.data}"
