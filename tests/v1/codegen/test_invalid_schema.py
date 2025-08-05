from io import StringIO

import pytest

from replit_river.codegen.client import schema_to_river_client_codegen


def test_field_name_collision_error() -> None:
    """Test that codegen raises ValueError for field name collisions."""

    with pytest.raises(ValueError) as exc_info:
        schema_to_river_client_codegen(
            read_schema=lambda: open("tests/v1/codegen/rpc/invalid-schema.json"),
            target_path="tests/v1/codegen/rpc/generated",
            client_name="InvalidClient",
            typed_dict_inputs=True,
            file_opener=lambda _: StringIO(),
            method_filter=None,
            protocol_version="v1.1",
        )

    # Check that the error message matches the expected format for field name collision
    error_message = str(exc_info.value)
    assert "Field name collision" in error_message
    assert "data:3" in error_message
    assert "data-3" in error_message
    assert "all normalize to the same effective name 'data_3'" in error_message
