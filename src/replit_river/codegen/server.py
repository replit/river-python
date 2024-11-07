import collections
import os.path
import subprocess
import tempfile
from textwrap import dedent
from typing import DefaultDict, Sequence

import grpc_tools  # type: ignore
from google.protobuf import descriptor_pb2
from google.protobuf.descriptor import FieldDescriptor
from grpc_tools import protoc

from replit_river.codegen.format import reindent


def to_camel_case(snake_str: str) -> str:
    """Converts a string in snake_case to camelCase."""
    components = snake_str.split("_")
    return components[0] + "".join(x.title() for x in components[1:])


def first_letter_uppercase(s: str) -> str:
    return s[0].upper() + s[1:]


def field_type_name(field: descriptor_pb2.FieldDescriptorProto) -> str:
    return field.type_name.split(".")[-1]


def get_encoder_name(field: descriptor_pb2.FieldDescriptorProto) -> str:
    return f"_{first_letter_uppercase(to_camel_case(field_type_name(field)))}Encoder"


def get_decoder_name(field: descriptor_pb2.FieldDescriptorProto) -> str:
    return f"_{first_letter_uppercase(to_camel_case(field_type_name(field)))}Decoder"


def message_decoder(
    module_name: str,
    m: descriptor_pb2.DescriptorProto,
) -> Sequence[str]:
    """Generates the lines of a River -> protobuf decoder."""
    chunks = [
        dedent(
            f"""\
        def _{m.name}Decoder(
          d: Mapping[str, Any],
        ) -> {module_name}_pb2.{m.name}:
          m = {module_name}_pb2.{m.name}()
          if d is None:
            return m
        """
        ),
    ]
    # Non-oneof fields.
    oneofs: DefaultDict[int, list[descriptor_pb2.FieldDescriptorProto]] = (
        collections.defaultdict(list)
    )
    for field in m.field:
        if field.HasField("oneof_index"):
            oneofs[field.oneof_index].append(field)
            continue
        chunks.append(
            f"  if d.get({repr(to_camel_case(field.name))}) is not None:",
        )
        if field.type_name == ".google.protobuf.Timestamp":
            chunks.append(
                reindent(
                    "    ",
                    f"""\
                _{field.name} = timestamp_pb2.Timestamp()
                _{field.name}.FromDatetime(d[{repr(to_camel_case(field.name))}])
                m.{field.name}.MergeFrom(_{field.name})
                """,
                )
            )
        elif field.type_name == ".google.protobuf.BoolValue":
            chunks.append(
                reindent(
                    "    ",
                    f"""\
                _{field.name} = BoolValue()
                _{field.name}.value = d[{repr(to_camel_case(field.name))}]
                m.{field.name}.MergeFrom(_{field.name})
                """,
                )
            )
        elif field.label == FieldDescriptor.LABEL_REPEATED:
            if field.type == descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE:
                decode_method_name = get_decoder_name(field)
                chunks.append(
                    reindent(
                        "    ",
                        f"""\
                    m.{field.name}.extend(
                        {decode_method_name}(item)
                        for item in d[{repr(to_camel_case(field.name))}]
                    )
                    """,
                    )
                )
            else:
                chunks.append(
                    reindent(
                        "    ",
                        f"""\
                    m.{field.name}.MergeFrom(d[{repr(to_camel_case(field.name))}])
                    """,
                    )
                )
        elif field.type == descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE:
            decode_method_name = get_decoder_name(field)
            chunks.append(
                reindent(
                    "    ",
                    f"""\
                m.{field.name}.MergeFrom(
                    {decode_method_name}(d[{repr(to_camel_case(field.name))}])
                )
                """,
                ),
            )
        else:
            chunks.append(
                reindent(
                    "    ",
                    f"""\
                setattr(
                    m,
                    {repr(field.name)},
                    d[{repr(to_camel_case(field.name))}]
                )
                """,
                )
            )
    # oneof fields.
    for index, oneof in enumerate(m.oneof_decl):
        chunks.extend(
            [
                reindent(
                    "  ",
                    f"""\
                _{oneof.name} = d.get({repr(to_camel_case(oneof.name))}, {{}})
                if _{oneof.name}:
                    match _{oneof.name}.get('$kind', None):
                """,
                ),
            ]
        )
        for field in oneofs[index]:
            chunks.append(
                reindent(
                    "        ",
                    f"""\
                    case {repr(to_camel_case(field.name))}:
                    """,
                ),
            )
            if field.type_name == ".google.protobuf.Timestamp":
                chunks.append(
                    reindent(
                        "          ",
                        f"""\
                    _{field.name} = timestamp_pb2.Timestamp()
                    _{field.name}.FromDatetime(
                        _{oneof.name}[{repr(to_camel_case(field.name))}],
                    )
                    m.{field.name}.MergeFrom(_{field.name})
                    """,
                    )
                )
            elif field.label == FieldDescriptor.LABEL_REPEATED:
                chunks.append(
                    reindent(
                        "          ",
                        f"""\
                    m.{field.name}.MergeFrom(
                        _{oneof.name}[{repr(to_camel_case(field.name))}]
                    )
                    """,
                    )
                )
            elif field.type == descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE:
                decode_method_name = get_decoder_name(field)
                chunks.append(
                    reindent(
                        "          ",
                        f"""\
                    m.{field.name}.MergeFrom(
                        {decode_method_name}(_{oneof.name}[{repr(to_camel_case(field.name))}])
                    )
                    """,
                    )
                )
            else:
                chunks.append(
                    reindent(
                        "          ",
                        f"""\
                    setattr(
                        m,
                        {repr(field.name)},
                        _{oneof.name}[{repr(to_camel_case(field.name))}],
                    )
                    """,
                    )
                )
    chunks.extend(
        [
            "  return m",
            "",
        ]
    )
    return chunks


def message_encoder(
    module_name: str,
    m: descriptor_pb2.DescriptorProto,
) -> Sequence[str]:
    """Generates the lines of a protobuf -> River encoder."""
    chunks = [
        dedent(
            f"""\
        def _{m.name}Encoder(
            e: {module_name}_pb2.{m.name}
        ) -> dict[str, Any]:
          d: dict[str, Any] = {{}}
        """
        ),
    ]
    # Non-oneof fields.
    oneofs: DefaultDict[int, list[descriptor_pb2.FieldDescriptorProto]] = (
        collections.defaultdict(list)
    )
    for field in m.field:
        if field.HasField("oneof_index"):
            oneofs[field.oneof_index].append(field)
            continue
        value: str
        if field.type_name == ".google.protobuf.Timestamp":
            value = f"_{field.name}.ToDatetime(tzinfo=datetime.timezone.utc)"
        elif field.type_name == ".google.protobuf.BoolValue":
            value = f"_{field.name}.value"
        elif field.label == FieldDescriptor.LABEL_REPEATED:
            if field.type == descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE:
                encode_method_name = get_encoder_name(field)
                value = f"[{encode_method_name}(item) for item in _{field.name}]"
            else:
                value = f"list(_{field.name})"
        elif field.type == descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE:
            field.type_name
            encode_method_name = get_encoder_name(field)
            value = f"{encode_method_name}(_{field.name})"
        else:
            value = f"_{field.name}"
        chunks.append(
            reindent(
                "  ",
                f"""\
            _{field.name} = e.{field.name}
            if _{field.name} is not None:
              d[{repr(to_camel_case(field.name))}] = {value}
            """,
            )
        )
    # oneof fields.
    for index, oneof in enumerate(m.oneof_decl):
        chunks.append(f"  match e.WhichOneof({repr(oneof.name)}):")
        for field in oneofs[index]:
            if field.type_name == ".google.protobuf.Timestamp":
                value = f"e.{field.name}.ToDatetime(tzinfo=datetime.timezone.utc)"
            elif field.type == descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE:
                encode_method_name = get_encoder_name(field)
                value = f"{encode_method_name}(e.{field.name})"
            else:
                value = f"e.{field.name}"
            chunks.append(
                reindent(
                    "    ",
                    f"""\
                case {repr(field.name)}:
                  d[{repr(to_camel_case(oneof.name))}] = {{
                    '$kind': {repr(to_camel_case(field.name))},
                    {repr(to_camel_case(field.name))}: {value},
                  }}
                """,
                )
            )
    chunks.extend(
        [
            "  return d",
            "",
        ]
    )
    return chunks


def generate_river_module(
    module_name: str,
    pb_module_name: str,
    fds: descriptor_pb2.FileDescriptorSet,
) -> Sequence[str]:
    """Generates the lines of a River module."""
    chunks: list[str] = [
        dedent(
            f"""\
        # ruff: noqa
        # Code generated by river.codegen. DO NOT EDIT.
        import datetime
        from typing import Any, Mapping

        from google.protobuf import timestamp_pb2
        from google.protobuf.wrappers_pb2 import BoolValue

        import replit_river as river

        from {module_name} import {pb_module_name}_pb2, {pb_module_name}_pb2_grpc
        """
        ),
        "",
        "",
    ]
    for pd in fds.file:

        def _remove_namespace(name: str) -> str:
            return name.replace(f".{pd.package}.", "")

        # Generate the message encoders/decoders.
        for message in pd.message_type:
            chunks.extend(message_encoder(pb_module_name, message))
            chunks.extend(message_decoder(pb_module_name, message))

        # Generate the service stubs.
        for service in pd.service:
            chunks.append(
                dedent(
                    f"""\
                def add_{service.name}Servicer_to_server(
                    servicer: {pb_module_name}_pb2_grpc.{service.name}Servicer,
                    server: river.Server,
                ) -> None:
                  rpc_method_handlers: Mapping[
                    tuple[str, str],
                    tuple[str, river.GenericRpcHandler]
                  ] = {{
                """
                ),
            )
            for method in service.method:
                service_name = "".join([service.name[0].lower(), service.name[1:]])
                method_name = "".join([method.name[0].lower(), method.name[1:]])
                descriptor = f"({repr(service_name)}, {repr(method_name)})"
                method_kind: str
                handler_name: str
                if method.client_streaming:
                    if method.server_streaming:
                        method_kind = "stream"
                        handler_name = "stream_method_handler"
                    else:
                        method_kind = "upload-stream"
                        handler_name = "upload_method_handler"
                else:
                    if method.server_streaming:
                        method_kind = "subscription-stream"
                        handler_name = "subscription_method_handler"
                    else:
                        method_kind = "rpc"
                        handler_name = "rpc_method_handler"

                decoder_name = f"_{_remove_namespace(method.input_type)}Decoder"
                encoder_name = f"_{_remove_namespace(method.output_type)}Encoder"

                chunks.append(
                    reindent(
                        "    ",
                        f"""\
                    {descriptor}: (
                        {repr(method_kind)},
                        river.{handler_name}(
                            servicer.{method.name},
                            {decoder_name},
                            {encoder_name},
                        ),
                    ),
                    """,
                    ),
                )
            chunks.append("  }")
            chunks.append("  server.add_rpc_handlers(rpc_method_handlers)")
            chunks.append("")
    return chunks


def proto_to_river_server_codegen(
    module_name: str,
    proto_path: str,
    target_directory: str,
) -> None:
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
    pb_module_name = os.path.splitext(os.path.basename(proto_path))[0]
    contents = "\n".join(generate_river_module(module_name, pb_module_name, fds))
    os.makedirs(target_directory, exist_ok=True)
    output_path = f"{target_directory}/{pb_module_name}_river.py"
    with open(output_path, "w") as f:
        popen = subprocess.Popen(
            ["ruff", "format", "-"], stdin=subprocess.PIPE, stdout=f
        )
        popen.communicate(contents.encode())
