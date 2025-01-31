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
    NewType,
    NotRequired,
    Union,
    Tuple,
    TypedDict,
)

from pydantic import BaseModel, Field, TypeAdapter
from replit_river.error_schema import RiverError

import replit_river as river


NeedsenumInput = Literal["in_first"] | Literal["in_second"]
encode_NeedsenumInput: Callable[["NeedsenumInput"], Any] = lambda x: x
NeedsenumOutputAnyOf__Unknown = NewType("NeedsenumOutputAnyOf__Unknown", object)
NeedsenumOutput = (
    Literal["out_first"] | Literal["out_second"] | NeedsenumOutputAnyOf__Unknown
)
NeedsenumErrorsAnyOf__Unknown = NewType("NeedsenumErrorsAnyOf__Unknown", object)
NeedsenumErrors = (
    Literal["err_first"] | Literal["err_second"] | NeedsenumErrorsAnyOf__Unknown
)
