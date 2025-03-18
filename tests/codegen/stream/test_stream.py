import importlib
from typing import AsyncIterable

import pytest
from pytest_snapshot.plugin import Snapshot

from replit_river.client import Client
from tests.codegen.snapshot.codegen_snapshot_fixtures import validate_codegen
from tests.common_handlers import basic_stream


@pytest.mark.parametrize("handlers", [{**basic_stream}])
async def test_basic_stream(snapshot: Snapshot, client: Client) -> None:
    validate_codegen(
        snapshot=snapshot,
        read_schema=lambda: open("tests/codegen/stream/schema.json"),
        target_path="test_basic_stream",
        client_name="StreamClient",
    )

    import tests.codegen.snapshot.snapshots.test_basic_stream

    importlib.reload(tests.codegen.snapshot.snapshots.test_basic_stream)
    from tests.codegen.snapshot.snapshots.test_basic_stream import (
        StreamClient,  # noqa: E501
    )
    from tests.codegen.snapshot.snapshots.test_basic_stream.test_service.stream_method import (  # noqa: E501
        Stream_MethodInput,
        Stream_MethodOutput,
    )

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
