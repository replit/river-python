import importlib
import shutil
from pathlib import Path
from typing import AsyncIterable, TextIO

import pytest

from replit_river.client import Client
from replit_river.codegen.client import schema_to_river_client_codegen
from tests.codegen.stream.generated.test_service.stream_method import (
    Stream_MethodInput,
    Stream_MethodOutput,
)
from tests.common_handlers import basic_stream


@pytest.fixture(scope="session", autouse=True)
def generate_stream_client() -> None:
    import tests.codegen.stream.generated

    shutil.rmtree("tests/codegen/stream/generated")

    def file_opener(path: Path) -> TextIO:
        return open(path, "w")

    schema_to_river_client_codegen(
        lambda: open("tests/codegen/stream/schema.json"),
        "tests/codegen/stream/generated",
        "StreamClient",
        True,
        file_opener,
    )
    importlib.reload(tests.codegen.stream.generated)


@pytest.mark.asyncio
@pytest.mark.parametrize("handlers", [{**basic_stream}])
async def test_basic_stream(client: Client) -> None:
    from tests.codegen.stream.generated import StreamClient

    async def emit() -> AsyncIterable[Stream_MethodInput]:
        for i in range(5):
            yield {"data": str(i)}

    res = await StreamClient(client).test_service.stream_method(emit())

    i = 0
    async for datum in res:
        assert isinstance(datum, Stream_MethodOutput)
        assert f"Stream response for {i}" == datum.data, f"{i} == {datum.data}"
        i = i + 1
    assert i == 5
