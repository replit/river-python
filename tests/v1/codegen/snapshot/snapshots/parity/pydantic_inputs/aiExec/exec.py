# Code generated by river.codegen. DO NOT EDIT.
from collections.abc import AsyncIterable, AsyncIterator
import datetime
from typing import (
    Any,
    Literal,
    Mapping,
    NotRequired,
    TypedDict,
)
from typing_extensions import Annotated

from pydantic import BaseModel, Field, TypeAdapter, WrapValidator
from replit_river.error_schema import RiverError
from replit_river.client import (
    RiverUnknownError,
    translate_unknown_error,
    RiverUnknownValue,
    translate_unknown_value,
)

import replit_river as river


class ExecInit(BaseModel):
    args: list[str]
    cwd: str | None = None
    env: dict[str, str] | None = None
    omitStderr: bool | None = None
    omitStdout: bool | None = None
    useReplitRunEnv: bool | None = None


class ExecInput(BaseModel):
    kind: Annotated[Literal["stdin"], Field(alias="$kind")] = "stdin"
    stdin: bytes


class ExecOutput(BaseModel):
    pass


ExecOutputTypeAdapter: TypeAdapter[ExecOutput] = TypeAdapter(ExecOutput)


class ExecErrors(RiverError):
    pass


ExecErrorsTypeAdapter: TypeAdapter[ExecErrors] = TypeAdapter(ExecErrors)


ExecInitTypeAdapter: TypeAdapter[ExecInit] = TypeAdapter(ExecInit)


ExecInputTypeAdapter: TypeAdapter[ExecInput] = TypeAdapter(ExecInput)
