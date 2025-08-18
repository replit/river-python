import asyncio
import importlib
import os
import shutil
from datetime import datetime, timedelta, timezone
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
def generate_special_chars_client() -> None:
    shutil.rmtree("tests/v1/codegen/rpc/generated_special_chars", ignore_errors=True)
    os.makedirs("tests/v1/codegen/rpc/generated_special_chars")

    def file_opener(path: Path) -> TextIO:
        return open(path, "w")

    schema_to_river_client_codegen(
        read_schema=lambda: open(
            "tests/v1/codegen/rpc/input-special-chars-schema.json"
        ),  # noqa: E501
        target_path="tests/v1/codegen/rpc/generated_special_chars",
        client_name="SpecialCharsClient",
        typed_dict_inputs=True,
        file_opener=file_opener,
        method_filter=None,
        protocol_version="v1.1",
    )


@pytest.fixture(scope="session", autouse=True)
def reload_rpc_import(
    generate_rpc_client: None, generate_special_chars_client: None
) -> None:  # noqa: E501
    import tests.v1.codegen.rpc.generated
    import tests.v1.codegen.rpc.generated_special_chars

    importlib.reload(tests.v1.codegen.rpc.generated)
    importlib.reload(tests.v1.codegen.rpc.generated_special_chars)


@pytest.mark.asyncio
@pytest.mark.parametrize("handlers", [{**basic_rpc_method}])
async def test_basic_rpc(client: Client) -> None:
    from tests.v1.codegen.rpc.generated import RpcClient

    res = await RpcClient(client).test_service.rpc_method(
        {
            "data": "feep",
            "data2": datetime.now(timezone.utc),
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


def deserialize_special_chars_request(request: dict) -> dict:
    """Deserialize request for special chars test - pass through the full dict."""
    return request


def serialize_special_chars_response(response: bool) -> dict:
    """Serialize response for special chars test."""
    return response


async def special_chars_handler(
    request: dict, context: grpc.aio.ServicerContext
) -> bool:  # noqa: E501
    """Handler that processes input with special character field names."""
    # The request comes with original field names (with special characters)
    # as they are sent over the wire before normalization

    # Verify we received the required fields with their original names
    required_fields = ["data-field1", "data:field2"]

    for field in required_fields:
        if field not in request:
            raise ValueError(
                f"Missing required field: {field}. Available keys: {list(request.keys())}"
            )  # noqa: E501

    # Verify the values are of expected types
    if not isinstance(request["data-field1"], str):
        raise ValueError("data-field1 should be a string")
    if not isinstance(request["data:field2"], (int, float)):
        raise ValueError("data:field2 should be a number")

    # Return True if all expected data is present and valid
    return True


special_chars_method: HandlerMapping = {
    ("test_service", "rpc_method"): (
        "rpc",
        rpc_method_handler(
            special_chars_handler,
            deserialize_special_chars_request,
            serialize_special_chars_response,
        ),
    )
}


@pytest.mark.asyncio
@pytest.mark.parametrize("handlers", [{**rpc_timeout_method}])
async def test_rpc_timeout(client: Client) -> None:
    from tests.v1.codegen.rpc.generated import RpcClient

    with pytest.raises(RiverException):
        await RpcClient(client).test_service.rpc_method(
            {"data": "feep", "data2": datetime.now(timezone.utc)},
            timedelta(milliseconds=200),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("handlers", [{**special_chars_method}])
async def test_special_chars_rpc(client: Client) -> None:
    """Test RPC method with special characters in field names."""
    from tests.v1.codegen.rpc.generated_special_chars import SpecialCharsClient

    # Test with all fields including optional ones
    result = await SpecialCharsClient(client).test_service.rpc_method(
        {
            "data_field1": "test_value1",  # Required: data-field1 -> data_field1
            "data_field2": 42.5,  # Required: data:field2 -> data_field2
            "data_field3": True,  # Optional: data.field3 -> data_field3
            "data_field4": "test_value4",  # Optional: data/field4 -> data_field4
            "data_field5": 123,  # Optional: data@field5 -> data_field5
            "data_field6": "test_value6",  # Optional: data field6 -> data_field6
        },
        timedelta(seconds=5),
    )
    assert result is True

    # Test with only required fields
    result = await SpecialCharsClient(client).test_service.rpc_method(
        {
            "data_field1": "required_value",
            "data_field2": 99.9,
        },
        timedelta(seconds=5),
    )
    assert result is True
