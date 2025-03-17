"""Generates the JSON schema of a River  from a proto file."""

import collections
import json
import os.path
import tempfile
from typing import Any, DefaultDict

import grpc_tools  # type: ignore
from google.protobuf import descriptor_pb2
from grpc_tools import protoc


def to_camel_case(snake_str: str) -> str:
    """Converts a string in snake_case to camelCase."""
    components = snake_str.split("_")
    return components[0] + "".join(x.title() for x in components[1:])


def first_letter_uppercase(s: str) -> str:
    return s[0].upper() + s[1:]


def field_type_name(field: descriptor_pb2.FieldDescriptorProto) -> str:
    return field.type_name.split(".")[-1]


def message_type(
    module_name: str,
    m: descriptor_pb2.DescriptorProto,
    sender: bool,
) -> dict[str, Any]:
    """Generates the type of a protobuf message into Typebox descriptions."""
    type: dict[str, Any] = {
        "type": "object",
        "properties": {},
        "required": [],
    }
    # Non-oneof fields.
    oneofs: DefaultDict[int, list[descriptor_pb2.FieldDescriptorProto]] = (
        collections.defaultdict(list)
    )
    for field in m.field:
        if field.HasField("oneof_index"):
            oneofs[field.oneof_index].append(field)
            continue
        if field.type == descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE:
            # TODO: implement
            pass
        elif field.type == descriptor_pb2.FieldDescriptorProto.TYPE_STRING:
            type["properties"][field.name] = {"type": "string"}
        else:
            raise Exception(f"unsupported field type: {field}")
        if not sender:
            # Protobuf message have this little semantic quirk that all fields are
            # optional from the perspective of the sender, but required and always
            # populated with the zero value (is missing) from the perspective of the
            # receiver. In this case, the client is acting as a sender.
            type["required"].append(field.name)
    return type


def generate_river_schema(
    module_name: str,
    fds: descriptor_pb2.FileDescriptorSet,
) -> list[dict[str, Any]]:
    """Generates the JSON schema of a River module."""
    service_schemas: list[dict[str, Any]] = []

    message_types: dict[str, descriptor_pb2.DescriptorProto] = {}

    for pd in fds.file:
        for message in pd.message_type:
            message_types[message.name] = message

    for pd in fds.file:

        def _remove_namespace(name: str) -> str:
            return name.replace(f".{pd.package}.", "")

        # Generate the service stubs.
        for service in pd.service:
            service_schema: dict[str, Any] = {
                "name": "".join([service.name[0].lower(), service.name[1:]]),
                "state": {},
                "procedures": {},
            }
            for method in service.method:
                method_kind: str
                if method.client_streaming:
                    if method.server_streaming:
                        method_kind = "stream"
                    else:
                        method_kind = "upload-stream"
                else:
                    if method.server_streaming:
                        method_kind = "subscription-stream"
                    else:
                        method_kind = "rpc"

                method_description = {
                    "type": method_kind,
                    "input": message_type(
                        module_name,
                        message_types[_remove_namespace(method.input_type)],
                        sender=True,
                    ),
                    "output": message_type(
                        module_name,
                        message_types[_remove_namespace(method.output_type)],
                        sender=False,
                    ),
                    # TODO: Add the correct gRPC error type instead of marking these
                    # as infallible.
                    "errors": {
                        "not": {},
                    },
                }
                service_schema["procedures"][
                    "".join([method.name[0].lower(), method.name[1:]])
                ] = method_description
            service_schemas.append(service_schema)

    return service_schemas


def proto_to_river_schema_codegen(proto_path: str, target_directory: str) -> None:
    fds = descriptor_pb2.FileDescriptorSet()
    with tempfile.TemporaryDirectory() as tempdir:
        descriptor_path = os.path.join(tempdir, "descriptor.pb")
        protoc.main(
            [
                f"--proto_path={os.path.dirname(proto_path)}",
                proto_path,
                f"--descriptor_set_out={descriptor_path}",
                "--include_source_info",
                f"-I{os.path.dirname(proto_path)}",
                f"-I{os.path.join(list(grpc_tools.__path__)[0], '_proto')}",
            ]
        )
        with open(descriptor_path, "rb") as f:
            fds.ParseFromString(f.read())
    module_name = os.path.splitext(os.path.basename(proto_path))[0]
    contents = json.dumps(generate_river_schema(module_name, fds), indent="  ")
    os.makedirs(target_directory, exist_ok=True)
    output_path = f"{target_directory}/{module_name}_schema.json"
    with open(output_path, "w") as f:
        f.write(contents)
