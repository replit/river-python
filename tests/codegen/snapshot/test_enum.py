from io import StringIO
from pathlib import Path
from typing import TextIO

from pytest_snapshot.plugin import Snapshot

from replit_river.codegen.client import schema_to_river_client_codegen

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


class UnclosableStringIO(StringIO):
    def close(self) -> None:
        pass


def test_unknown_enum(snapshot: Snapshot) -> None:
    snapshot.snapshot_dir = "tests/codegen/snapshot/snapshots"
    files: dict[Path, UnclosableStringIO] = {}

    def file_opener(path: Path) -> TextIO:
        buffer = UnclosableStringIO()
        assert path not in files, "Codegen attempted to write to the same file twice!"
        files[path] = buffer
        return buffer

    schema_to_river_client_codegen(
        read_schema=lambda: StringIO(test_unknown_enum_schema),
        target_path="test_unknown_enum",
        client_name="foo",
        file_opener=file_opener,
        typed_dict_inputs=True,
    )
    for path, file in files.items():
        file.seek(0)
        snapshot.assert_match(file.read(), Path(snapshot.snapshot_dir, path))
