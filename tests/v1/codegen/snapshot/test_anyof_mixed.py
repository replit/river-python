from pytest_snapshot.plugin import Snapshot

from tests.fixtures.codegen_snapshot_fixtures import validate_codegen


async def test_anyof_mixed_types(snapshot: Snapshot) -> None:
    """Test codegen for anyOf unions with mixed types (object, string, array).

    This tests the fix for the bug where non-discriminated anyOf unions
    with mixed types like [object, string, array] would generate malformed
    Python code with broken ternary expressions.
    """
    validate_codegen(
        snapshot=snapshot,
        snapshot_dir="tests/v1/codegen/snapshot/snapshots",
        read_schema=lambda: open("tests/v1/codegen/types/anyof_mixed_schema.json"),
        target_path="test_anyof_mixed_types",
        client_name="AnyOfMixedClient",
        protocol_version="v1.1",
    )
