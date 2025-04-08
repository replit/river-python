import importlib
from typing import AsyncIterable, Literal

import pytest
from pytest_snapshot.plugin import Snapshot

from replit_river.client import Client, RiverUnknownError
from tests.fixtures.codegen_snapshot_fixtures import validate_codegen
from tests.v1.common_handlers import basic_stream, error_stream

_AlreadyGenerated = False


@pytest.fixture
def stream_client_codegen(snapshot: Snapshot) -> Literal[True]:
    global _AlreadyGenerated
    if not _AlreadyGenerated:
        validate_codegen(
            snapshot=snapshot,
            snapshot_dir="tests/v1/codegen/snapshot/snapshots",
            read_schema=lambda: open("tests/v1/codegen/stream/schema.json"),
            target_path="test_basic_stream",
            client_name="StreamClient",
            protocol_version="v1.1",
        )
        _AlreadyGenerated = True

    import tests.v1.codegen.snapshot.snapshots.test_basic_stream

    importlib.reload(tests.v1.codegen.snapshot.snapshots.test_basic_stream)
    return True


@pytest.mark.parametrize("handlers", [{**basic_stream}])
async def test_basic_stream(
    stream_client_codegen: Literal[True],
    client: Client,
) -> None:
    from tests.v1.codegen.snapshot.snapshots.test_basic_stream import (
        StreamClient,  # noqa: E501
    )
    from tests.v1.codegen.snapshot.snapshots.test_basic_stream.test_service.stream_method import (  # noqa: E501
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


@pytest.mark.parametrize("handlers", [{**error_stream}])
@pytest.mark.parametrize("phase", [0, 1, 2, 3])
async def test_error_stream(
    stream_client_codegen: Literal[True],
    erroringClient: Client,
    phase: int,
) -> None:
    from tests.v1.codegen.snapshot.snapshots.test_basic_stream import (
        StreamClient,  # noqa: E501
    )

    async def emit() -> AsyncIterable[int]:
        yield phase

    res = await StreamClient(erroringClient).test_service.emit_error(emit())

    async for datum in res:
        match phase:
            case 0:
                assert datum
            case 1:
                assert not datum
            case 2:
                assert not isinstance(datum, bool)
                assert datum.code == "DATA_LOSS"
            case 3:
                assert isinstance(datum, RiverUnknownError)
                assert datum.code == "UNIMPLEMENTED"
