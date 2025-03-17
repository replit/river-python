import importlib
from pathlib import Path
from typing import AsyncIterable, TextIO

import pytest
from pytest_snapshot.plugin import Snapshot

from replit_river.client import Client
from replit_river.codegen.client import schema_to_river_client_codegen
from tests.codegen.snapshot.test_enum import UnclosableStringIO
from tests.common_handlers import basic_stream


@pytest.mark.parametrize("handlers", [{**basic_stream}])
async def test_basic_stream(snapshot: Snapshot, client: Client) -> None:
    snapshot.snapshot_dir = "tests/codegen/snapshot/snapshots"
    files: dict[Path, UnclosableStringIO] = {}

    def file_opener(path: Path) -> TextIO:
        buffer = UnclosableStringIO()
        assert path not in files, "Codegen attempted to write to the same file twice!"
        files[path] = buffer
        return buffer

    schema_to_river_client_codegen(
        read_schema=lambda: open("tests/codegen/stream/schema.json"),
        target_path="test_basic_stream",
        client_name="StreamClient",
        file_opener=file_opener,
        typed_dict_inputs=True,
    )

    for path, file in files.items():
        file.seek(0)
        snapshot.assert_match(file.read(), Path(snapshot.snapshot_dir, path))

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
