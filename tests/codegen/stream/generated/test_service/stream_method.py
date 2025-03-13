# ruff: noqa
# Code generated by river.codegen. DO NOT EDIT.
from collections.abc import AsyncIterable, AsyncIterator
import datetime
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Literal,
    Optional,
    Mapping,
    NotRequired,
    Union,
    Tuple,
    TypedDict,
)
from typing_extensions import Annotated

from pydantic import BaseModel, Field, TypeAdapter, WrapValidator
from replit_river.error_schema import RiverError
from replit_river.client import RiverUnknownValue, translate_unknown_value

import replit_river as river


encode_Stream_MethodInput: Callable[["Stream_MethodInput"], Any] = lambda x: {
    k: v
    for (k, v) in (
        {
            "data": x.get("data"),
        }
    ).items()
    if v is not None
}


class Stream_MethodInput(TypedDict):
    data: str


Stream_MethodInputTypeAdapter = TypeAdapter(Stream_MethodInput)  # type: ignore


class Stream_MethodOutput(BaseModel):
    data: str


Stream_MethodOutputTypeAdapter = TypeAdapter(Stream_MethodOutput)  # type: ignore
