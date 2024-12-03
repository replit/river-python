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
    Union,
    Tuple,
    TypedDict,
)

from pydantic import BaseModel, Field, TypeAdapter
from replit_river.error_schema import RiverError

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


class Stream_MethodOutput(BaseModel):
    data: str
