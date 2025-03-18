from io import StringIO

from pytest_snapshot.plugin import Snapshot

from tests.codegen.snapshot.codegen_snapshot_fixtures import validate_codegen

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
                "type": "string",
                "const": "err_first"
              },
              {
                "type": "string",
                "const": "err_second"
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
        read_schema=lambda: StringIO(test_unknown_enum_schema),
        target_path="test_unknown_enum",
        client_name="foo",
    )
