from io import StringIO

import pytest

from replit_river.codegen.client import schema_to_river_client_codegen


def test_input_special_chars_basemodel() -> None:
    """Test that codegen handles special characters in input field names for BaseModel."""
    
    # Test should pass without raising an exception
    schema_to_river_client_codegen(
        read_schema=lambda: open("tests/v1/codegen/rpc/input-special-chars-schema.json"),
        target_path="tests/v1/codegen/rpc/generated_input_special",
        client_name="InputSpecialClient",
        typed_dict_inputs=False,  # BaseModel inputs
        file_opener=lambda _: StringIO(),
        method_filter=None,
        protocol_version="v1.1",
    )


def test_input_special_chars_typeddict() -> None:
    """Test that codegen handles special characters in input field names for TypedDict."""
    
    # Test should pass without raising an exception
    schema_to_river_client_codegen(
        read_schema=lambda: open("tests/v1/codegen/rpc/input-special-chars-schema.json"),
        target_path="tests/v1/codegen/rpc/generated_input_special_td",
        client_name="InputSpecialTDClient",
        typed_dict_inputs=True,  # TypedDict inputs
        file_opener=lambda _: StringIO(),
        method_filter=None,
        protocol_version="v1.1",
    )


def test_input_collision_error_basemodel() -> None:
    """Test that codegen raises ValueError for input field name collisions with BaseModel."""

    with pytest.raises(ValueError) as exc_info:
        schema_to_river_client_codegen(
            read_schema=lambda: open("tests/v1/codegen/rpc/input-collision-schema.json"),
            target_path="tests/v1/codegen/rpc/generated_input_collision",
            client_name="InputCollisionClient",
            typed_dict_inputs=False,  # BaseModel inputs
            file_opener=lambda _: StringIO(),
            method_filter=None,
            protocol_version="v1.1",
        )

    # Check that the error message matches the expected format for field name collision
    error_message = str(exc_info.value)
    assert "Field name collision" in error_message
    assert "data-3" in error_message
    assert "data:3" in error_message
    assert "all normalize to the same effective name 'data_3'" in error_message


def test_input_collision_error_typeddict() -> None:
    """Test that codegen raises ValueError for input field name collisions with TypedDict."""

    with pytest.raises(ValueError) as exc_info:
        schema_to_river_client_codegen(
            read_schema=lambda: open("tests/v1/codegen/rpc/input-collision-schema.json"),
            target_path="tests/v1/codegen/rpc/generated_input_collision_td",
            client_name="InputCollisionTDClient",
            typed_dict_inputs=True,  # TypedDict inputs
            file_opener=lambda _: StringIO(),
            method_filter=None,
            protocol_version="v1.1",
        )

    # Check that the error message matches the expected format for field name collision
    error_message = str(exc_info.value)
    assert "Field name collision" in error_message
    assert "data-3" in error_message
    assert "data:3" in error_message
    assert "all normalize to the same effective name 'data_3'" in error_message


def test_init_special_chars_basemodel() -> None:
    """Test that codegen handles special characters in init field names for BaseModel."""
    
    init_schema = {
        "services": {
            "test_service": {
                "procedures": {
                    "stream_method": {
                        "init": {
                            "type": "object",
                            "properties": {
                                "init-field1": {"type": "string"},
                                "init:field2": {"type": "number"},
                                "init.field3": {"type": "boolean"}
                            },
                            "required": ["init-field1"]
                        },
                        "output": {"type": "boolean"},
                        "errors": {"not": {}},
                        "type": "stream"
                    }
                }
            }
        }
    }
    
    import json
    
    # Test should pass without raising an exception
    schema_to_river_client_codegen(
        read_schema=lambda: StringIO(json.dumps(init_schema)),
        target_path="tests/v1/codegen/rpc/generated_init_special",
        client_name="InitSpecialClient",
        typed_dict_inputs=False,  # BaseModel inputs
        file_opener=lambda _: StringIO(),
        method_filter=None,
        protocol_version="v2.0",
    )


def test_init_special_chars_typeddict() -> None:
    """Test that codegen handles special characters in init field names for TypedDict."""
    
    init_schema = {
        "services": {
            "test_service": {
                "procedures": {
                    "stream_method": {
                        "init": {
                            "type": "object",
                            "properties": {
                                "init-field1": {"type": "string"},
                                "init:field2": {"type": "number"},
                                "init.field3": {"type": "boolean"}
                            },
                            "required": ["init-field1"]
                        },
                        "output": {"type": "boolean"},
                        "errors": {"not": {}},
                        "type": "stream"
                    }
                }
            }
        }
    }
    
    import json
    
    # Test should pass without raising an exception
    schema_to_river_client_codegen(
        read_schema=lambda: StringIO(json.dumps(init_schema)),
        target_path="tests/v1/codegen/rpc/generated_init_special_td",
        client_name="InitSpecialTDClient",
        typed_dict_inputs=True,  # TypedDict inputs
        file_opener=lambda _: StringIO(),
        method_filter=None,
        protocol_version="v2.0",
    )
