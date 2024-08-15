import json
import re
from typing import Any, Dict, List, Literal, Optional, Sequence, Set, Tuple, Union

import black
from pydantic import BaseModel, Field, RootModel

_NON_ALNUM_RE = re.compile(r"[^a-zA-Z0-9_]+")
_LITERAL_RE = re.compile(r"^Literal\[(.+)\]$")


class RiverConcreteType(BaseModel):
    type: Optional[str] = Field(default=None)
    properties: Dict[str, "RiverType"] = Field(default_factory=lambda: dict())
    required: Set[str] = Field(default=set())
    items: Optional["RiverType"] = Field(default=None)
    const: Optional[Union[str, int]] = Field(default=None)
    patternProperties: Dict[str, "RiverType"] = Field(default_factory=lambda: dict())


class RiverUnionType(BaseModel):
    anyOf: List["RiverType"]


class RiverNotType(BaseModel):
    """This is used to represent void / never."""

    not_: Any = Field(..., alias="not")


RiverType = Union[RiverConcreteType, RiverUnionType, RiverNotType]


class RiverProcedure(BaseModel):
    init: Optional[RiverType] = Field(default=None)
    input: RiverType
    output: RiverType
    errors: Optional[RiverType] = Field(default=None)
    type: (
        Literal["rpc"] | Literal["stream"] | Literal["subscription"] | Literal["upload"]
    )
    description: Optional[str] = Field(default=None)


class RiverSchema(BaseModel):
    procedures: Dict[str, RiverProcedure]


RiverSchemaFile = RootModel[Dict[str, RiverSchema]]


def encode_type(
    type: RiverType, prefix: str, base_model: str = "BaseModel"
) -> Tuple[str, Sequence[str]]:
    chunks: List[str] = []
    if isinstance(type, RiverNotType):
        return ("None", ())
    if isinstance(type, RiverUnionType):
        # First check if it's a discriminated union. Typebox currently doesn't have
        # a way of expressing the intention of having a discriminated union. So we
        # do a bit of detection if that is structurally true by checking that all the
        # types in the anyOf are objects, have properties, and have one property common
        # to all the alternatives that has a literal value.
        one_of_candidate_types: List[RiverConcreteType] = [
            t
            for t in type.anyOf
            if isinstance(t, RiverConcreteType)
            and t.type == "object"
            and t.properties
            and (not t.patternProperties or "^(.*)$" not in t.patternProperties)
        ]
        if len(type.anyOf) > 0 and len(type.anyOf) == len(one_of_candidate_types):
            # We have established that it is a union-of-objects. Now let's see if
            # there is a discriminator field common among all options.
            literal_fields = set[str]()
            for i, oneof_t in enumerate(one_of_candidate_types):
                lf = set[str](
                    name
                    for name, prop in oneof_t.properties.items()
                    if isinstance(prop, RiverConcreteType)
                    and prop.type in ("string", "number", "boolean")
                    and prop.const is not None
                )
                if i == 0:
                    literal_fields = lf
                else:
                    literal_fields.intersection_update(lf)
                if not literal_fields:
                    # There are no more candidates.
                    break
            if len(literal_fields) == 1:
                # Hooray! we found a discriminated union.
                discriminator_name = literal_fields.pop()
                one_of: List[str] = []

                for oneof_t in one_of_candidate_types:
                    discriminator_value = [
                        _NON_ALNUM_RE.sub("", str(prop.const))
                        for name, prop in oneof_t.properties.items()
                        if isinstance(prop, RiverConcreteType)
                        and name == discriminator_name
                        and prop.const is not None
                    ].pop()
                    type_name, type_chunks = encode_type(
                        oneof_t, f"{prefix}OneOf_{discriminator_value}", base_model
                    )
                    chunks.extend(type_chunks)
                    one_of.append(type_name)
                if discriminator_name == "$kind":
                    discriminator_name = "kind"
                chunks.append(f"{prefix} = Union[" + ", ".join(one_of) + "]")
                chunks.append("")
                return (prefix, chunks)
        any_of: List[str] = []
        for i, t in enumerate(type.anyOf):
            type_name, type_chunks = encode_type(t, f"{prefix}AnyOf_{i}", base_model)
            chunks.extend(type_chunks)
            any_of.append(type_name)
        chunks.append(f"{prefix} = Union[" + ", ".join(any_of) + "]")
        return (prefix, chunks)
    if isinstance(type, RiverConcreteType):
        if type.type is None:
            # Handle the case where type is not specified
            return ("Any", ())
        if type.type == "string":
            if type.const:
                return (f"Literal['{type.const}']", ())
            else:
                return ("str", ())
        if type.type == "Uint8Array":
            return ("bytes", ())
        if type.type == "number":
            if type.const is not None:
                # enums are represented as const number in the schema
                return (f"Literal[{type.const}]", ())
            return ("float", ())
        if type.type == "integer":
            if type.const is not None:
                # enums are represented as const number in the schema
                return (f"Literal[{type.const}]", ())
            return ("int", ())
        if type.type == "boolean":
            return ("bool", ())
        if type.type == "null":
            return ("None", ())
        if type.type == "Date":
            return ("datetime.datetime", ())
        if type.type == "array" and type.items:
            type_name, type_chunks = encode_type(type.items, prefix, base_model)
            return (f"List[{type_name}]", type_chunks)
        if (
            type.type == "object"
            and type.patternProperties
            and "^(.*)$" in type.patternProperties
        ):
            type_name, type_chunks = encode_type(
                type.patternProperties["^(.*)$"], prefix, base_model
            )
            return (f"Dict[str, {type_name}]", type_chunks)
        assert type.type == "object", type.type
        current_chunks: List[str] = [f"class {prefix}({base_model}):"]
        if type.properties:
            for name, prop in type.properties.items():
                type_name, type_chunks = encode_type(
                    prop, prefix + name.title(), base_model
                )
                chunks.extend(type_chunks)
                if name == "$kind":
                    # If the field is a literal, the Python type-checker will complain
                    # about the constructor not being able to specify a value for it:
                    # You can't put `$kind="ok"` in the ctor because `$` is not a valid
                    # character in an identifier, and putting `**{"$kind":"ok"}` makes
                    # it not recognize the `"ok"` as being `Literal["ok"]`, so we're
                    # stuck with an impossible-to-construct object.
                    field_value = "..."
                    groups = _LITERAL_RE.match(type_name)
                    if groups:
                        field_value = groups.group(1)
                    if name not in type.required:
                        current_chunks.append(
                            f"  kind: Optional[{type_name}] = "
                            f"Field({field_value}, alias='{name}', default=None)"
                        )
                    else:
                        current_chunks.append(
                            f"  kind: {type_name} = "
                            f"Field({field_value}, alias='{name}')"
                        )
                else:
                    if name not in type.required:
                        current_chunks.append(f"  {name}: Optional[{type_name}] = None")
                    else:
                        current_chunks.append(f"  {name}: {type_name}")
        else:
            current_chunks.append("  pass")
        current_chunks.append("")
        chunks.extend(current_chunks)

    return (prefix, chunks)


def generate_river_client_module(
    client_name: str,
    schemas: Dict[str, RiverSchema],
) -> Sequence[str]:
    chunks: List[str] = [
        "# Code generated by river.codegen. DO NOT EDIT.",
        "from collections.abc import AsyncIterable, AsyncIterator",
        "import datetime",
        "from typing import Any, Dict, List, Literal, Optional, Mapping, Union, Tuple",
        "",
        "from pydantic import BaseModel, Field, TypeAdapter",
        "from replit_river.error_schema import RiverError",
        "",
        "import replit_river as river",
        "",
    ]
    for schema_name, schema in schemas.items():
        current_chunks: List[str] = [
            f"class {schema_name.title()}Service:",
            "  def __init__(self, client: river.Client):",
            "    self.client = client",
            "",
        ]
        for name, procedure in schema.procedures.items():
            init_type: Optional[str] = None
            if procedure.init:
                init_type, input_chunks = encode_type(
                    procedure.init, f"{schema_name.title()}{name.title()}Init"
                )
                chunks.extend(input_chunks)
            input_type, input_chunks = encode_type(
                procedure.input, f"{schema_name.title()}{name.title()}Input"
            )
            chunks.extend(input_chunks)
            output_type, output_chunks = encode_type(
                procedure.output, f"{schema_name.title()}{name.title()}Output"
            )
            chunks.extend(output_chunks)
            if procedure.errors:
                error_type, errors_chunks = encode_type(
                    procedure.errors,
                    f"{schema_name.title()}{name.title()}Errors",
                    base_model="RiverError",
                )
                if error_type == "None":
                    error_type = "RiverError"
                    output_or_error_type = f"Union[{output_type}, {error_type}]"
                    error_encoder = f"TypeAdapter({error_type}).validate_python(x)"
                else:
                    output_or_error_type = f"Union[{output_type}, {error_type}]"
                    error_encoder = f"TypeAdapter({error_type}).validate_python(x)"
                    chunks.extend(errors_chunks)
            else:
                error_type = "RiverError"
                output_or_error_type = f"Union[{output_type}, {error_type}]"
                error_encoder = f"TypeAdapter({error_type}).validate_python(x)"

            output_encoder = f"TypeAdapter({output_type}).validate_python(x)"
            if output_type == "None":
                output_encoder = "None"
            # TODO: mypy ignore is added because TypeAdapter(...).validate_python
            # cannot handle Union types, it should be fixed by making
            # parse_output_method type aware.
            parse_output_method = (
                f"lambda x: {output_encoder}, # type: ignore[arg-type]"
            )
            parse_error_method = f"lambda x: {error_encoder}, # type: ignore[arg-type]"

            if procedure.type == "rpc":
                control_flow_keyword = "return "
                if output_type == "None":
                    control_flow_keyword = ""
                current_chunks.extend(
                    [
                        f"  async def {name}(",
                        "    self,",
                        f"    input: {input_type},",
                        f"  ) -> {output_type}:"
                        f"    {control_flow_keyword}await self.client.send_rpc(",
                        f"      '{schema_name}',",
                        f"      '{name}',",
                        "      input,",
                        f"      lambda x: TypeAdapter({input_type}).dump_python(x, ",
                        "        by_alias=True,",
                        "        exclude_none=True,",
                        "      ),",
                        f"     {parse_output_method}",
                        f"     {parse_error_method}",
                        "    )",
                    ]
                )
            elif procedure.type == "subscription":
                current_chunks.extend(
                    [
                        f"  async def {name}(",
                        "    self,",
                        f"    input: {input_type},",
                        f") -> AsyncIterator[{output_or_error_type}]:",
                        "    return await self.client.send_subscription(",
                        f"      '{schema_name}',",
                        f"      '{name}',",
                        "      input,",
                        f"      lambda x: TypeAdapter({input_type}).dump_python(x,",
                        "        by_alias=True,",
                        "        exclude_none=True,",
                        "      ),",
                        f"     {parse_output_method}",
                        f"     {parse_error_method}",
                        "    )",
                    ]
                )
            elif procedure.type == "upload":
                control_flow_keyword = "return "
                output_encoder = f"TypeAdapter({output_type}).validate_python(x)"
                if output_type == "None":
                    control_flow_keyword = ""
                    output_encoder = "None"
                if init_type:
                    current_chunks.extend(
                        [
                            f"  async def {name}(",
                            "    self,",
                            f"    init: {init_type},",
                            f"    inputStream: AsyncIterable[{input_type}],",
                            f"  ) -> {output_type}:",
                            f"    {control_flow_keyword}await self.client.send_upload(",
                            f"      '{schema_name}',",
                            f"      '{name}',",
                            "      init,",
                            "      inputStream,",
                            f"      TypeAdapter({init_type}).validate_python,",
                            f"      lambda x: TypeAdapter({input_type}).dump_python(x,",
                            "        by_alias=True,",
                            "        exclude_none=True,",
                            "      ),",
                            f"     {parse_output_method}",
                            f"     {parse_error_method}",
                            "    )",
                        ]
                    )
                else:
                    current_chunks.extend(
                        [
                            f"  async def {name}(",
                            "    self,",
                            f"    inputStream: AsyncIterable[{input_type}],"
                            f"  ) -> {output_or_error_type}:",
                            f"    {control_flow_keyword}await self.client.send_upload(",
                            f"      '{schema_name}',",
                            f"      '{name}',",
                            "     None,",
                            "     inputStream,",
                            "     None,",
                            f"      lambda x: TypeAdapter({input_type}).dump_python(x,",
                            "        by_alias=True,",
                            "        exclude_none=True,",
                            "      ),",
                            f"     {parse_output_method}",
                            f"     {parse_error_method}",
                            "    )",
                        ]
                    )
            elif procedure.type == "stream":
                if init_type:
                    current_chunks.extend(
                        [
                            f"  async def {name}(",
                            "    self,",
                            f"    init: {init_type},",
                            f"    inputStream: AsyncIterable[{input_type}],",
                            f"  ) -> AsyncIterator[{output_or_error_type}]:",
                            "    return await self.client.send_stream(",
                            f"      '{schema_name}',",
                            f"      '{name}',",
                            "      init,",
                            "      inputStream,",
                            f"     TypeAdapter({init_type}).validate_python,",
                            f"      lambda x: TypeAdapter({input_type}).dump_python(x,",
                            "        by_alias=True,",
                            "        exclude_none=True,",
                            "      ),",
                            f"     {parse_output_method}",
                            f"     {parse_error_method}",
                            "    )",
                        ]
                    )
                else:
                    current_chunks.extend(
                        [
                            f"  async def {name}(",
                            "    self,",
                            f"    inputStream: AsyncIterable[{input_type}],",
                            f") -> AsyncIterator[{output_or_error_type}]:",
                            "    return await self.client.send_stream(",
                            f"      '{schema_name}',",
                            f"      '{name}',",
                            "      None,",
                            "      inputStream,",
                            "      None,",
                            f"      lambda x: TypeAdapter({input_type}).dump_python(x,",
                            "        by_alias=True,",
                            "        exclude_none=True,",
                            "      ),",
                            f"     {parse_output_method}",
                            f"     {parse_error_method}",
                            "    )",
                        ]
                    )

            current_chunks.append("")
        chunks.extend(current_chunks)

    chunks.extend(
        [
            f"class {client_name}:",
            "  def __init__(self, client: river.Client):",
        ]
    )
    for schema_name, schema in schemas.items():
        chunks.append(
            f"    self.{schema_name} = {schema_name.title()}Service(client)",
        )

    return chunks


def schema_to_river_client_codegen(
    schema_path: str, target_path: str, client_name: str
) -> None:
    """Generates the lines of a River module."""
    with open(schema_path) as f:
        schemas = RiverSchemaFile(json.load(f))
    with open(target_path, "w") as f:
        f.write(
            black.format_str(
                "\n".join(generate_river_client_module(client_name, schemas.root)),
                mode=black.FileMode(string_normalization=False),
            )
        )
