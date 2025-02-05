import argparse
import os.path
from pathlib import Path
from typing import TextIO

from .client import schema_to_river_client_codegen
from .schema import proto_to_river_schema_codegen
from .server import proto_to_river_server_codegen


def main() -> None:
    parser = argparse.ArgumentParser("River codegen")
    subparsers = parser.add_subparsers(dest="command", required=True)

    server = subparsers.add_parser(
        "server", help="Codegen a River server from gRPC protos"
    )
    server.add_argument(
        "--module", dest="module_name", help="output module", default="."
    )
    server.add_argument("--output", help="output directory", required=True)
    server.add_argument("proto", help="proto file")

    server_schema = subparsers.add_parser(
        "server-schema", help="Codegen a River server schema from gRPC protos"
    )
    server_schema.add_argument("--output", help="output directory", required=True)
    server_schema.add_argument("proto", help="proto file")

    client = subparsers.add_parser(
        "client", help="Codegen a River client from JSON schema"
    )
    client.add_argument("--output", help="output path", required=True)
    client.add_argument("--client-name", help="name of the class", required=True)
    client.add_argument(
        "--typed-dict-inputs",
        help="Enable typed dicts",
        action="store_true",
        default=False,
    )
    client.add_argument("schema", help="schema file")
    args = parser.parse_args()

    if args.command == "server":
        proto_path = os.path.abspath(args.proto)
        target_directory = os.path.abspath(args.output)
        proto_to_river_server_codegen(args.module_name, proto_path, target_directory)
    elif args.command == "server-schema":
        proto_path = os.path.abspath(args.proto)
        target_directory = os.path.abspath(args.output)
        proto_to_river_schema_codegen(proto_path, target_directory)
    elif args.command == "client":
        schema_path = os.path.abspath(args.schema)
        target_path = os.path.abspath(args.output)

        def file_opener(path: Path) -> TextIO:
            return open(path, "w")

        schema_to_river_client_codegen(
            lambda: open(schema_path),
            target_path,
            args.client_name,
            args.typed_dict_inputs,
            file_opener,
        )
    else:
        raise NotImplementedError(f"Unknown command {args.command}")
