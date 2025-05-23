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


class SpawnInputSize(BaseModel):
    cols: int
    rows: int


class SpawnInput(BaseModel):
    args: list[str] | None = None
    autoCleanup: bool | None = None
    cmd: str
    cwd: str | None = None
    env: dict[str, str] | None = None
    initialCmd: str | None = None
    interactive: bool | None = None
    size: SpawnInputSize | None = None
    useCgroupMagic: bool | None = None
    useReplitRunEnv: bool | None = None


class SpawnOutput(BaseModel):
    pass


SpawnOutputTypeAdapter: TypeAdapter[SpawnOutput] = TypeAdapter(SpawnOutput)


class SpawnErrors(RiverError):
    pass


SpawnErrorsTypeAdapter: TypeAdapter[SpawnErrors] = TypeAdapter(SpawnErrors)


SpawnInputTypeAdapter: TypeAdapter[SpawnInput] = TypeAdapter(SpawnInput)
