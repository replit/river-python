import json
import re
from textwrap import dedent, indent
from typing import (
    Any,
    Literal,
    Optional,
    OrderedDict,
    Sequence,
    Set,
    Tuple,
    Union,
    cast,
)

import black
from pydantic import BaseModel, Field, RootModel

_NON_ALNUM_RE = re.compile(r"[^a-zA-Z0-9_]+")
_LITERAL_RE = re.compile(r"^Literal\[(.+)\]$")


class RiverConcreteType(BaseModel):
    type: Optional[str] = Field(default=None)
    properties: dict[str, "RiverType"] = Field(default_factory=lambda: dict())
    required: Set[str] = Field(default=set())
    items: Optional["RiverType"] = Field(default=None)
    const: Optional[Union[str, int]] = Field(default=None)
    patternProperties: dict[str, "RiverType"] = Field(default_factory=lambda: dict())


class RiverUnionType(BaseModel):
    anyOf: list["RiverType"]


class RiverIntersectionType(BaseModel):
    allOf: list["RiverType"]


class RiverNotType(BaseModel):
    """This is used to represent void / never."""

    not_: Any = Field(..., alias="not")


RiverType = Union[
    RiverConcreteType, RiverUnionType, RiverNotType, RiverIntersectionType
]


class RiverProcedure(BaseModel):
    init: RiverType
    input: Optional[RiverType] = Field(default=None)
    output: RiverType
    errors: Optional[RiverType] = Field(default=None)
    type: (
        Literal["rpc"] | Literal["stream"] | Literal["subscription"] | Literal["upload"]
    )
    description: Optional[str] = Field(default=None)


class RiverService(BaseModel):
    procedures: dict[str, RiverProcedure]


class RiverSchema(BaseModel):
    services: dict[str, RiverService]
    handshakeSchema: Optional[RiverConcreteType] = Field(default=None)


RiverSchemaFile = RootModel[RiverSchema]


def reindent(prefix: str, code: str) -> str:
    """
    Take an arbitrarily indented code block, dedent to the lowest common
    indent level and then reindent based on the supplied prefix
    """
    return indent(dedent(code), prefix)


def is_literal(tpe: RiverType) -> bool:
    if isinstance(tpe, RiverUnionType):
        return all(is_literal(t) for t in tpe.anyOf)
    elif isinstance(tpe, RiverConcreteType):
        return tpe.type not in set(["object", "array"])
    else:
        return False


def encode_type(
    type: RiverType, prefix: str, base_model: str
) -> Tuple[str, Sequence[str]]:
    chunks: list[str] = []
    if isinstance(type, RiverNotType):
        return ("None", ())
    if isinstance(type, RiverUnionType):
        typeddict_encoder = list[str]()

        # First check if it's a discriminated union. Typebox currently doesn't have
        # a way of expressing the intention of having a discriminated union. So we
        # do a bit of detection if that is structurally true by checking that all the
        # types in the anyOf are objects, have properties, and have one property common
        # to all the alternatives that has a literal value.
        def flatten_union(tpe: RiverType) -> list[RiverType]:
            if isinstance(tpe, RiverUnionType):
                return [u for t in tpe.anyOf for u in flatten_union(t)]
            else:
                return [tpe]

        original_type = type

        type = RiverUnionType(anyOf=flatten_union(type))

        one_of_candidate_types: list[RiverConcreteType] = [
            t
            for _t in type.anyOf
            for t in (_t.anyOf if isinstance(_t, RiverUnionType) else [_t])
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
                one_of_pending = OrderedDict[str, tuple[str, list[RiverConcreteType]]]()

                for oneof_t in one_of_candidate_types:
                    discriminator_value = [
                        _NON_ALNUM_RE.sub("", str(prop.const))
                        for name, prop in oneof_t.properties.items()
                        if isinstance(prop, RiverConcreteType)
                        and name == discriminator_name
                        and prop.const is not None
                    ].pop()
                    one_of_pending.setdefault(
                        f"{prefix}OneOf_{discriminator_value}",
                        (discriminator_value, []),
                    )[1].append(oneof_t)

                one_of: list[str] = []
                if discriminator_name == "$kind":
                    discriminator_name = "kind"
                for pfx, (discriminator_value, oneof_ts) in one_of_pending.items():
                    if len(oneof_ts) > 1:
                        typeddict_encoder.append("(")
                        # Tricky bit. We need to derive our own discriminator based
                        # on known members. Be careful.

                        common_members = set[str]()
                        for i, oneof_t in enumerate(oneof_ts):
                            if i == 0:
                                common_members = set(oneof_t.properties.keys())
                            else:
                                common_members.intersection_update(
                                    oneof_t.properties.keys()
                                )

                        for i, oneof_t in enumerate(oneof_ts):
                            type_name, type_chunks = encode_type(
                                oneof_ts[i], f"{pfx}{i}", base_model
                            )
                            chunks.extend(type_chunks)
                            one_of.append(type_name)
                            local_discriminators = set(
                                oneof_t.properties.keys()
                            ).difference(common_members)
                            typeddict_encoder.append(
                                f"""
                                encode_{type_name}(x) # type: ignore[arg-type]
                            """.strip()
                            )
                            if local_discriminators:
                                local_discriminator = sorted(local_discriminators).pop()
                            else:
                                local_discriminator = "FIXME: Ambiguous discriminators"
                            typeddict_encoder.append(
                                f" if {repr(local_discriminator)} in x else "
                            )
                        typeddict_encoder.pop()  # Drop the last ternary
                        typeddict_encoder.append(")")
                    else:
                        oneof_t = oneof_ts[0]
                        type_name, type_chunks = encode_type(oneof_t, pfx, base_model)
                        chunks.extend(type_chunks)
                        one_of.append(type_name)
                        typeddict_encoder.append(f"encode_{type_name}(x)")
                    typeddict_encoder.append(
                        f"""
                            if x[{repr(discriminator_name)}]
                            == {repr(discriminator_value)}
                            else
                        """,
                    )
                chunks.append(f"{prefix} = Union[" + ", ".join(one_of) + "]")
                chunks.append("")

                if base_model == "TypedDict":
                    chunks.extend(
                        [
                            dedent(
                                f"""\
                    def encode_{prefix}(x: {repr(prefix)}) -> Any:
                        return (
                            """
                            ),
                        ]
                        + typeddict_encoder[:-1]  # Drop the last ternary
                        + [")"]
                    )
                return (prefix, chunks)
            # End of stable union detection
        # Restore the non-flattened union type
        type = original_type
        any_of: list[str] = []

        typeddict_encoder = []
        for i, t in enumerate(type.anyOf):
            type_name, type_chunks = encode_type(t, f"{prefix}AnyOf_{i}", base_model)
            chunks.extend(type_chunks)
            any_of.append(type_name)
            if isinstance(t, RiverConcreteType):
                if t.type == "string":
                    typeddict_encoder.extend(["x", " if isinstance(x, str) else "])
                else:
                    typeddict_encoder.append(f"encode_{type_name}(x)")
        if is_literal(type):
            typeddict_encoder = ["x"]
        chunks.append(f"{prefix} = Union[" + ", ".join(any_of) + "]")
        if base_model == "TypedDict":
            chunks.extend(
                [
                    dedent(
                        f"""\
                    def encode_{prefix}(x: {repr(prefix)}) -> Any:
                        return (
                            """
                    ),
                ]
                + typeddict_encoder
                + [")"]
            )
        return (prefix, chunks)
    if isinstance(type, RiverIntersectionType):

        def extract_props(tpe: RiverType) -> list[dict[str, RiverType]]:
            if isinstance(tpe, RiverUnionType):
                return [t for p in tpe.anyOf for t in extract_props(p)]
            elif isinstance(tpe, RiverConcreteType):
                return [tpe.properties]
            elif isinstance(tpe, RiverIntersectionType):
                return [t for p in tpe.allOf for t in extract_props(p)]
            else:
                return []

        combined = {}
        for x in extract_props(type):
            combined.update(x)
        return encode_type(
            RiverConcreteType(type="object", properties=combined),
            prefix=prefix,
            base_model=base_model,
        )
    if isinstance(type, RiverConcreteType):
        typeddict_encoder = list[str]()
        if type.type is None:
            # Handle the case where type is not specified
            typeddict_encoder.append("x")
            return ("Any", ())
        elif type.type == "string":
            if type.const:
                typeddict_encoder.append(f"{repr(type.const)}")
                return (f"Literal[{repr(type.const)}]", ())
            else:
                typeddict_encoder.append("x")
                return ("str", ())
        elif type.type == "Uint8Array":
            typeddict_encoder.append("x.decode()")
            return ("bytes", ())
        elif type.type == "number":
            if type.const is not None:
                # enums are represented as const number in the schema
                typeddict_encoder.append(f"{type.const}")
                return (f"Literal[{type.const}]", ())
            typeddict_encoder.append("x")
            return ("float", ())
        elif type.type == "integer":
            if type.const is not None:
                # enums are represented as const number in the schema
                typeddict_encoder.append(f"{type.const}")
                return (f"Literal[{type.const}]", ())
            typeddict_encoder.append("x")
            return ("int", ())
        elif type.type == "boolean":
            typeddict_encoder.append("x")
            return ("bool", ())
        elif type.type == "null" or type.type == "undefined":
            typeddict_encoder.append("None")
            return ("None", ())
        elif type.type == "Date":
            typeddict_encoder.append("TODO: dstewart")
            return ("datetime.datetime", ())
        elif type.type == "array" and type.items:
            type_name, type_chunks = encode_type(type.items, prefix, base_model)
            typeddict_encoder.append("TODO: dstewart")
            return (f"list[{type_name}]", type_chunks)
        elif (
            type.type == "object"
            and type.patternProperties
            and "^(.*)$" in type.patternProperties
        ):
            type_name, type_chunks = encode_type(
                type.patternProperties["^(.*)$"], prefix, base_model
            )
            typeddict_encoder.append(f"encode_{type_name}(x)")
            return (f"dict[str, {type_name}]", type_chunks)
        assert type.type == "object", type.type

        current_chunks: list[str] = [f"class {prefix}({base_model}):"]
        # For the encoder path, do we need "x" to be bound?
        # lambda x: ... vs lambda _: {}
        needs_binding = False
        if type.properties:
            needs_binding = True
            typeddict_encoder.append("{")
            for name, prop in type.properties.items():
                typeddict_encoder.append(f"{repr(name)}:")

                type_name, type_chunks = encode_type(
                    prop, prefix + name.title(), base_model
                )
                chunks.extend(type_chunks)

                if base_model == "TypedDict":
                    if isinstance(prop, RiverNotType):
                        typeddict_encoder.append("'not implemented'")
                    elif isinstance(prop, RiverUnionType):
                        typeddict_encoder.append(f"encode_{type_name}(x[{repr(name)}])")
                        if name not in type.required:
                            typeddict_encoder.append(f"if x[{repr(name)}] else None")
                    elif isinstance(prop, RiverIntersectionType):
                        typeddict_encoder.append(f"encode_{type_name}(x[{repr(name)}])")
                    elif isinstance(prop, RiverConcreteType):
                        if name == "$kind":
                            safe_name = "kind"
                        else:
                            safe_name = name
                        if prop.type == "object" and not prop.patternProperties:
                            typeddict_encoder.append(
                                f"encode_{type_name}(x[{repr(safe_name)}])"
                            )
                            if name not in prop.required:
                                typeddict_encoder.append(
                                    dedent(
                                        f"""
                                        if {repr(safe_name)} in x
                                        and x[{repr(safe_name)}] is not None
                                        else None
                                    """
                                    )
                                )
                        elif prop.type == "array":
                            items = cast(RiverConcreteType, prop).items
                            assert items, "Somehow items was none"
                            if is_literal(cast(RiverType, items)):
                                typeddict_encoder.append(f"x[{repr(name)}]")
                            else:
                                assert type_name.startswith(
                                    "list["
                                )  # in case we change to list[...]
                                _inner_type_name = type_name[len("list[") : -len("]")]
                                typeddict_encoder.append(
                                    f"""[
                                        encode_{_inner_type_name}(y)
                                        for y in x[{repr(name)}]
                                        ]"""
                                )
                        else:
                            if name in prop.required:
                                typeddict_encoder.append(f"x[{repr(safe_name)}]")
                            else:
                                typeddict_encoder.append(f"x.get({repr(safe_name)})")

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
                        value = ""
                        if base_model != "TypedDict":
                            value = dedent(
                                f"""\
                                = Field(
                                  default=None,
                                  alias={repr(name)}, # type: ignore
                                )
                                """
                            )
                        current_chunks.append(f"  kind: Optional[{type_name}]{value}")
                    else:
                        value = ""
                        if base_model != "TypedDict":
                            value = dedent(
                                f"""\
                                = Field(
                                    {field_value},
                                    alias={repr(name)}, # type: ignore
                                )
                                """
                            )
                        current_chunks.append(f"  kind: {type_name}{value}")
                else:
                    if name not in type.required:
                        value = ""
                        if base_model != "TypedDict":
                            value = " = None"
                        current_chunks.append(f"  {name}: Optional[{type_name}]{value}")
                    else:
                        current_chunks.append(f"  {name}: {type_name}")
                typeddict_encoder.append(",")
            typeddict_encoder.append("}")
            # exclude_none
            typeddict_encoder = (
                ["{k: v for (k, v) in ("]
                + typeddict_encoder
                + [").items() if v is not None}"]
            )
        else:
            typeddict_encoder.append("{}")
            current_chunks.append("  pass")
        current_chunks.append("")

        if base_model == "TypedDict":
            binding = "x" if needs_binding else "_"
            current_chunks = (
                [
                    dedent(
                        f"""\
                    def encode_{prefix}({binding}: {repr(prefix)}) -> Any:
                        return (
                            """
                    ),
                ]
                + typeddict_encoder
                + [")"]
                + current_chunks
            )
        chunks.extend(current_chunks)

    return (prefix, chunks)


def generate_river_client_module(
    client_name: str,
    schema_root: RiverSchema,
    typed_dict_inputs: bool,
) -> Sequence[str]:
    chunks: list[str] = [
        dedent(
            """\
        # ruff: noqa
        # Code generated by river.codegen. DO NOT EDIT.
        from collections.abc import AsyncIterable, AsyncIterator
        import datetime
        from typing import (
            Any,
            Literal,
            Optional,
            Mapping,
            Union,
            Tuple,
            TypedDict,
        )

        from pydantic import BaseModel, Field, TypeAdapter
        from replit_river.error_schema import RiverError

        import replit_river as river

        """
        )
    ]

    if schema_root.handshakeSchema is not None:
        (handshake_type, handshake_chunks) = encode_type(
            schema_root.handshakeSchema, "HandshakeSchema", "BaseModel"
        )
        chunks.extend(handshake_chunks)
    else:
        handshake_type = "Literal[None]"

    input_base_class = "TypedDict" if typed_dict_inputs else "BaseModel"
    for schema_name, schema in schema_root.services.items():
        current_chunks: list[str] = [
            dedent(
                f"""\
                  class {schema_name.title()}Service:
                    def __init__(self, client: river.Client[{handshake_type}]):
                      self.client = client
                """
            ),
        ]
        for name, procedure in schema.procedures.items():
            init_type, init_chunks = encode_type(
                procedure.init,
                f"{schema_name.title()}{name.title()}Init",
                base_model=input_base_class,
            )
            chunks.extend(init_chunks)
            input_type: Optional[str] = None
            if procedure.input:
                input_type, input_chunks = encode_type(
                    procedure.input,
                    f"{schema_name.title()}{name.title()}Input",
                    base_model=input_base_class,
                )
                chunks.extend(input_chunks)
            output_type, output_chunks = encode_type(
                procedure.output,
                f"{schema_name.title()}{name.title()}Output",
                "BaseModel",
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
                else:
                    chunks.extend(errors_chunks)
            else:
                error_type = "RiverError"
            output_or_error_type = f"Union[{output_type}, {error_type}]"

            # NB: These strings must be indented to at least the same level of
            #     the function strings in the branches below, otherwise `dedent`
            #     will pick our indentation level for normalization, which will
            #     break the "def" indentation presuppositions.
            parse_output_method = f"""\
                                lambda x: TypeAdapter({output_type})
                                    .validate_python(
                                        x # type: ignore[arg-type]
                                    )
                """.rstrip()
            parse_error_method = f"""\
                                lambda x: TypeAdapter({error_type})
                                    .validate_python(
                                        x # type: ignore[arg-type]
                                    )
                """.rstrip()

            # Init renderer
            if typed_dict_inputs:
                if is_literal(procedure.init):
                    render_init_method = "lambda x: x"
                elif isinstance(
                    procedure.init, RiverConcreteType
                ) and procedure.init.type in ["array"]:
                    assert init_type.startswith(
                        "list["
                    )  # in case we change to list[...]
                    _init_type_name = init_type[len("list[") : -len("]")]
                    render_init_method = (
                        f"lambda xs: [encode_{_init_type_name}(x) for x in xs]"
                    )
                else:
                    render_init_method = f"encode_{init_type}"
            else:
                render_init_method = f"""\
                                lambda x: TypeAdapter({init_type})
                                  .validate_python
                """.rstrip()

            # Input renderer
            if typed_dict_inputs and input_type and procedure.input:
                if is_literal(procedure.input):
                    render_input_method = "lambda x: x"
                elif isinstance(
                    procedure.input, RiverConcreteType
                ) and procedure.input.type in ["array"]:
                    assert input_type.startswith(
                        "list["
                    )  # in case we change to list[...]
                    _input_type_name = input_type[len("list[") : -len("]")]
                    render_input_method = (
                        f"lambda xs: [encode_{_input_type_name}(x) for x in xs]"
                    )
                else:
                    render_input_method = f"encode_{input_type}"
            else:
                render_input_method = f"""\
                                lambda x: TypeAdapter({input_type})
                                  .dump_python(
                                    x, # type: ignore[arg-type]
                                    by_alias=True,
                                    exclude_none=True,
                                  )
                """.rstrip()
            if isinstance(
                procedure.input, RiverConcreteType
            ) and procedure.input.type not in ["object", "array"]:
                render_input_method = "lambda x: x"

            if output_type == "None":
                parse_output_method = "lambda x: None"

            if procedure.type == "rpc":
                current_chunks.extend(
                    [
                        reindent(
                            "  ",
                            f"""\
                    async def {name}(
                      self,
                      init: {init_type},
                    ) -> {output_type}:
                      return await self.client.send_rpc(
                        {repr(schema_name)},
                        {repr(name)},
                        init,
                        {render_init_method},
                        {parse_output_method},
                        {parse_error_method},
                      )
                        """,
                        )
                    ]
                )
            elif procedure.type == "subscription":
                current_chunks.extend(
                    [
                        reindent(
                            "  ",
                            f"""\
                    async def {name}(
                      self,
                      init: {init_type},
                    ) -> AsyncIterator[{output_or_error_type}]:
                      return await self.client.send_subscription(
                        {repr(schema_name)},
                        {repr(name)},
                        init,
                        {render_init_method},
                        {parse_output_method},
                        {parse_error_method},
                      )
                      """,
                        )
                    ]
                )
            elif procedure.type == "upload":
                if input_type is not None:
                    current_chunks.extend(
                        [
                            reindent(
                                "  ",
                                f"""\
                        async def {name}(
                          self,
                          init: {init_type},
                          inputStream: AsyncIterable[{input_type}],
                        ) -> {output_type}:
                          return await self.client.send_upload(
                            {repr(schema_name)},
                            {repr(name)},
                            init,
                            inputStream,
                            {render_init_method},
                            {render_input_method},
                            {parse_output_method},
                            {parse_error_method},
                          )
                            """,
                            )
                        ]
                    )
                else:
                    current_chunks.extend(
                        [
                            reindent(
                                "  ",
                                f"""\
                        async def {name}(
                          self,
                          init: {init_type},
                        ) -> {output_type}:
                          return await self.client.send_upload(
                            {repr(schema_name)},
                            {repr(name)},
                            init,
                            None,
                            {render_init_method},
                            None,
                            {parse_output_method},
                            {parse_error_method},
                          )
                            """,
                            )
                        ]
                    )
            elif procedure.type == "stream":
                if input_type is not None:
                    current_chunks.extend(
                        [
                            reindent(
                                "  ",
                                f"""\
                        async def {name}(
                          self,
                          init: {init_type},
                          inputStream: AsyncIterable[{input_type}],
                        ) -> AsyncIterator[{output_or_error_type}]:
                          return await self.client.send_stream(
                            {repr(schema_name)},
                            {repr(name)},
                            init,
                            inputStream,
                            {render_init_method},
                            {render_input_method},
                            {parse_output_method},
                            {parse_error_method},
                          )
                            """,
                            )
                        ]
                    )
                else:
                    current_chunks.extend(
                        [
                            reindent(
                                "  ",
                                f"""\
                        async def {name}(
                          self,
                          init: {init_type},
                        ) -> AsyncIterator[{output_or_error_type}]:
                          return await self.client.send_stream(
                            {repr(schema_name)},
                            {repr(name)},
                            init,
                            None,
                            {render_init_method},
                            None,
                            {parse_output_method},
                            {parse_error_method},
                          )
                            """,
                            )
                        ]
                    )

            current_chunks.append("")
        chunks.extend(current_chunks)

    chunks.extend(
        [
            dedent(
                f"""\
                class {client_name}:
                  def __init__(self, client: river.Client[{handshake_type}]):
                """.rstrip()
            )
        ]
    )
    for schema_name, schema in schema_root.services.items():
        chunks.append(
            f"    self.{schema_name} = {schema_name.title()}Service(client)",
        )

    return chunks


def schema_to_river_client_codegen(
    schema_path: str,
    target_path: str,
    client_name: str,
    typed_dict_inputs: bool,
) -> None:
    """Generates the lines of a River module."""
    with open(schema_path) as f:
        schemas = RiverSchemaFile(json.load(f))
    with open(target_path, "w") as f:
        s = "\n".join(
            generate_river_client_module(client_name, schemas.root, typed_dict_inputs)
        )
        try:
            f.write(
                black.format_str(s, mode=black.FileMode(string_normalization=False))
            )
        except:
            f.write(s)
            raise
