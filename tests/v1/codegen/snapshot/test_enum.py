import importlib
import json
from io import StringIO

from pytest_snapshot.plugin import Snapshot

from tests.fixtures.codegen_snapshot_fixtures import validate_codegen

test_unknown_enum_schema = """
{
  "services": {
    "enumService": {
      "procedures": {
        "needsEnum": {
          "type": "rpc",
          "input": {
            "anyOf": [
              {
                "type": "string",
                "const": "in_first"
              },
              {
                "type": "string",
                "const": "in_second"
              }
            ]
          },
          "output": {
            "anyOf": [
              {
                "type": "string",
                "const": "out_first"
              },
              {
                "type": "string",
                "const": "out_second"
              }
            ]
          },
          "errors": {
            "anyOf": [
              {
                "type": "object",
                "properties": {
                  "code": {
                    "const": "err_first",
                    "type": "string"
                  },
                  "message": {
                    "type": "string"
                  }
                },
                "required": ["code", "message"]
              },
              {
                "type": "object",
                "properties": {
                  "code": {
                    "const": "err_second",
                    "type": "string"
                  },
                  "message": {
                    "type": "string"
                  }
                },
                "required": ["code", "message"]
              }
            ]
          }
        },
        "needsEnumObject": {
          "type": "rpc",
          "input": {
            "anyOf": [
              {
                "type": "object",
                "properties": {
                  "$kind": {
                    "const": "in_first",
                    "type": "string"
                  },
                  "value": {
                    "type": "string"
                  }
                },
                "required": ["$kind", "value"]
              },
              {
                "type": "object",
                "properties": {
                  "$kind": {
                    "const": "in_second",
                    "type": "string"
                  },
                  "bleep": {
                    "type": "integer"
                  }
                },
                "required": ["$kind", "bleep"]
              }
            ]
          },
          "output": {
            "type": "object",
            "properties": {
              "foo": {
                "anyOf": [
                  {
                    "type": "object",
                    "properties": {
                      "$kind": {
                        "const": "out_first",
                        "type": "string"
                      },
                      "foo": {
                        "type": "integer"
                      }
                    },
                    "required": ["$kind", "foo"]
                  },
                  {
                    "type": "object",
                    "properties": {
                      "$kind": {
                        "const": "out_second",
                        "type": "string"
                      },
                      "bar": {
                        "type": "integer"
                      }
                    },
                    "required": ["$kind", "bar"]
                  }
                ]
              }
            }
          },
          "errors": {
            "type": "object",
            "properties": {
              "foo": {
                "anyOf": [
                  {
                    "type": "object",
                    "properties": {
                      "beep": {
                        "type": "string",
                        "const": "err_first"
                      }
                    }
                  },
                  {
                    "type": "object",
                    "properties": {
                      "borp": {
                        "type": "string",
                        "const": "err_second"
                      }
                    }
                  }
                ]
              }
            }
          }
        }
      }
    }
  }
}
"""


def test_unknown_enum(snapshot: Snapshot) -> None:
    validate_codegen(
        snapshot=snapshot,
        snapshot_dir="tests/v1/codegen/snapshot/snapshots",
        read_schema=lambda: StringIO(test_unknown_enum_schema),
        target_path="test_unknown_enum",
        client_name="foo",
        protocol_version="v1.1",
    )

    import tests.v1.codegen.snapshot.snapshots.test_unknown_enum

    importlib.reload(tests.v1.codegen.snapshot.snapshots.test_unknown_enum)
    from tests.v1.codegen.snapshot.snapshots.test_unknown_enum.enumService.needsEnum import (  # noqa
        NeedsenumErrorsTypeAdapter,
    )

    payloads: list[dict[str, str]] = [
        {
            "code": "err_first",
            "message": "This is a message",
        },
        {
            "code": "err_second",
            "message": "This is a message",
        },
        {
            "code": "unknown_error",
            "message": "This is new!",
        },
    ]

    for error in payloads:
        x = NeedsenumErrorsTypeAdapter.validate_python(error)
        assert x.code == error["code"]
        assert x.message == error["message"]


def test_unknown_enum_field_aliases(snapshot: Snapshot) -> None:
    validate_codegen(
        snapshot=snapshot,
        snapshot_dir="tests/v1/codegen/snapshot/snapshots",
        read_schema=lambda: StringIO(test_unknown_enum_schema),
        target_path="test_unknown_enum",
        client_name="foo",
        protocol_version="v1.1",
    )

    import tests.v1.codegen.snapshot.snapshots.test_unknown_enum

    importlib.reload(tests.v1.codegen.snapshot.snapshots.test_unknown_enum)
    from tests.v1.codegen.snapshot.snapshots.test_unknown_enum.enumService.needsEnumObject import (  # noqa
        NeedsenumobjectOutputTypeAdapter,
        NeedsenumobjectOutput,
        NeedsenumobjectOutputFooOneOf_out_first,
    )

    initial = NeedsenumobjectOutput(foo=NeedsenumobjectOutputFooOneOf_out_first(foo=5))
    result = NeedsenumobjectOutputTypeAdapter.dump_json(
        initial,
        by_alias=True,
    )

    obj = json.loads(result)

    # Make sure we are testing what we think we are testing
    assert "foo" in obj

    # We must not include the un-aliased field name
    assert "kind" not in obj["foo"]

    # We must include the aliased field name
    assert "$kind" in obj["foo"]

    # ... and finally that the values are what we think they should be
    assert obj["foo"]["$kind"] == "out_first"
    assert obj["foo"]["foo"] == 5

    # And one more sanity check for the decoder
    decoded = NeedsenumobjectOutputTypeAdapter.validate_json(result)
    assert decoded == initial
