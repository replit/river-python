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
from replit_river.client import RiverUnknownError, translate_unknown_error

import replit_river as river


NeedsenumInput = Literal["in_first"] | Literal["in_second"]
encode_NeedsenumInput: Callable[["NeedsenumInput"], Any] = lambda x: x

NeedsenumInputTypeAdapter: TypeAdapter[NeedsenumInput] = TypeAdapter(NeedsenumInput)

NeedsenumOutput = Annotated[
    Literal["out_first"] | Literal["out_second"] | RiverUnknownError,
    WrapValidator(translate_unknown_error),
]

NeedsenumOutputTypeAdapter: TypeAdapter[NeedsenumOutput] = TypeAdapter(NeedsenumOutput)


class NeedsenumErrorsOneOf_err_first(RiverError):
    code: Literal["err_first"]
    message: str


class NeedsenumErrorsOneOf_err_second(RiverError):
    code: Literal["err_second"]
    message: str


NeedsenumErrors = Annotated[
    NeedsenumErrorsOneOf_err_first
    | NeedsenumErrorsOneOf_err_second
    | RiverUnknownError,
    WrapValidator(translate_unknown_error),
]


NeedsenumErrorsTypeAdapter: TypeAdapter[NeedsenumErrors] = TypeAdapter(NeedsenumErrors)
