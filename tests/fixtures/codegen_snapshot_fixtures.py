from io import StringIO
from pathlib import Path
from typing import Callable, Literal, TextIO

from pytest_snapshot.plugin import Snapshot

from replit_river.codegen.client import schema_to_river_client_codegen


class UnclosableStringIO(StringIO):
    def close(self) -> None:
        pass


def validate_codegen(
    *,
    snapshot: Snapshot,
    snapshot_dir: str,
    read_schema: Callable[[], TextIO],
    target_path: str,
    client_name: str,
    protocol_version: Literal["v1.1", "v2.0"],
    typeddict_inputs: bool = True,
) -> None:
    snapshot.snapshot_dir = snapshot_dir
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
        typed_dict_inputs=typeddict_inputs,
        method_filter=None,
        protocol_version=protocol_version,
    )
    for path, file in files.items():
        file.seek(0)
        snapshot.assert_match(file.read(), Path(snapshot.snapshot_dir, path))
