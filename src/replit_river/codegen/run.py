import argparse
import os.path
import pathlib
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
    client.add_argument(
        "--method-filter",
        help="Only generate a subset of the specified methods",
        action="store",
        type=pathlib.Path,
    )
    client.add_argument(
        "--protocol-version",
        help="Generate river v2 clients",
        action="store",
        default="v1.1",
        choices=["v1.1", "v2.0"],
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

        method_filter: set[str] | None = None
        if args.method_filter:
            with open(args.method_filter) as handle:
                method_filter = set(x.strip() for x in handle.readlines())

        schema_to_river_client_codegen(
            read_schema=lambda: open(schema_path),
            target_path=target_path,
            client_name=args.client_name,
            typed_dict_inputs=args.typed_dict_inputs,
            file_opener=file_opener,
            method_filter=method_filter,
            protocol_version=args.protocol_version,
        )
    else:
        raise NotImplementedError(f"Unknown command {args.command}")
