from pytest_snapshot.plugin import Snapshot

from tests.fixtures.codegen_snapshot_fixtures import validate_codegen


async def test_anyof_array_in_union(snapshot: Snapshot) -> None:
    """Test codegen for an array field whose item type is a non-discriminated
    anyOf union that itself contains an `array` variant.

    Concretely this mirrors the PostgreSQL `executeSqlCommand.params` schema:
    `array<scalar | array<scalar>>`. The inner union encoder ends in an
    iteration over `x` (for the array variant), and historically that branch
    was emitted as the unguarded `else` of a ternary chain. When mypy failed
    to fully narrow `x` to `list[...]` through the preceding `isinstance`
    checks, it complained that scalar items of the union have no
    `__iter__` attribute (`union-attr`).

    The fix emits an explicit `isinstance(x, list)` guard for the array
    branch and a `cast(Any, x)` fallback, so mypy never has to negative-
    narrow into the iterating branch.
    """
    validate_codegen(
        snapshot=snapshot,
        snapshot_dir="tests/v1/codegen/snapshot/snapshots",
        read_schema=lambda: open(
            "tests/v1/codegen/types/anyof_array_in_union_schema.json"
        ),
        target_path="test_anyof_array_in_union",
        client_name="AnyOfArrayInUnionClient",
        protocol_version="v1.1",
    )
