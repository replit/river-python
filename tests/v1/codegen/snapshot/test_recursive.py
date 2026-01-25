from pytest_snapshot.plugin import Snapshot

from tests.fixtures.codegen_snapshot_fixtures import validate_codegen


async def test_recursive_types(snapshot: Snapshot) -> None:
    """Test that recursive types using $id/$ref are generated correctly."""
    validate_codegen(
        snapshot=snapshot,
        snapshot_dir="tests/v1/codegen/snapshot/snapshots",
        read_schema=lambda: open("tests/v1/codegen/types/recursive_schema.json"),
        target_path="test_recursive_types",
        client_name="RecursiveClient",
        protocol_version="v1.1",
    )
