from pytest_snapshot.plugin import Snapshot

from tests.codegen.snapshot.codegen_snapshot_fixtures import validate_codegen


async def test_pathological_types(snapshot: Snapshot) -> None:
    validate_codegen(
        snapshot=snapshot,
        read_schema=lambda: open("tests/codegen/types/schema.json"),
        target_path="test_pathological_types",
        client_name="PathologicalClient",
    )
