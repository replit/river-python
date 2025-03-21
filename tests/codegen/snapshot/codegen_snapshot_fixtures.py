from io import StringIO
from pathlib import Path
from typing import Callable, TextIO

from pytest_snapshot.plugin import Snapshot

from replit_river.codegen.client import schema_to_river_client_codegen


class UnclosableStringIO(StringIO):
    def close(self) -> None:
        pass


def validate_codegen(
    *,
    snapshot: Snapshot,
    read_schema: Callable[[], TextIO],
    target_path: str,
    client_name: str,
) -> None:
    snapshot.snapshot_dir = "tests/codegen/snapshot/snapshots"
    files: dict[Path, UnclosableStringIO] = {}

    def file_opener(path: Path) -> TextIO:
        buffer = UnclosableStringIO()
        assert path not in files, "Codegen attempted to write to the same file twice!"
        files[path] = buffer
        return buffer

    schema_to_river_client_codegen(
        read_schema=read_schema,
        target_path=target_path,
        client_name=client_name,
        file_opener=file_opener,
        typed_dict_inputs=True,
    )
    for path, file in files.items():
        file.seek(0)
        snapshot.assert_match(file.read(), Path(snapshot.snapshot_dir, path))
