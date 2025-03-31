from pytest_snapshot.plugin import Snapshot

from tests.fixtures.codegen_snapshot_fixtures import validate_codegen


async def test_pathological_types(snapshot: Snapshot) -> None:
    validate_codegen(
        snapshot=snapshot,
        snapshot_dir="tests/v1/codegen/snapshot/snapshots",
        read_schema=lambda: open("tests/v1/codegen/types/schema.json"),
        target_path="test_pathological_types",
        client_name="PathologicalClient",
        protocol_version="v1.1",
    )
