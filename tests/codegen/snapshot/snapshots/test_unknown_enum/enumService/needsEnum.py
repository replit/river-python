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


NeedsenumInput = Literal["in_first"] | Literal["in_second"]
encode_NeedsenumInput: Callable[["NeedsenumInput"], Any] = lambda x: x
NeedsenumOutput = Annotated[
    Literal["out_first"] | Literal["out_second"] | RiverUnknownValue,
    WrapValidator(translate_unknown_value),
]
NeedsenumErrors = Annotated[
    Literal["err_first"] | Literal["err_second"] | RiverUnknownValue,
    WrapValidator(translate_unknown_value),
]
