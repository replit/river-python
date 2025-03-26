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


def encode_Pathological_MethodInputObj_Date(
    x: "Pathological_MethodInputObj_Date",
) -> Any:
    return {
        k: v
        for (k, v) in (
            {
                "prop_Date": x.get("prop_Date"),
            }
        ).items()
        if v is not None
    }


class Pathological_MethodInputObj_Date(TypedDict):
    prop_Date: NotRequired[datetime.datetime | None]


def encode_Pathological_MethodInputObj_Uint8Array(
    x: "Pathological_MethodInputObj_Uint8Array",
) -> Any:
    return {
        k: v
        for (k, v) in (
            {
                "prop_Uint8Array": x.get("prop_Uint8Array"),
            }
        ).items()
        if v is not None
    }


class Pathological_MethodInputObj_Uint8Array(TypedDict):
    prop_Uint8Array: NotRequired[bytes | None]


def encode_Pathological_MethodInputObj_Boolean(
    x: "Pathological_MethodInputObj_Boolean",
) -> Any:
    return {
        k: v
        for (k, v) in (
            {
                "prop_boolean": x.get("prop_boolean"),
            }
        ).items()
        if v is not None
    }


class Pathological_MethodInputObj_Boolean(TypedDict):
    prop_boolean: NotRequired[bool | None]


def encode_Pathological_MethodInputObj_Integer(
    x: "Pathological_MethodInputObj_Integer",
) -> Any:
    return {
        k: v
        for (k, v) in (
            {
                "prop_integer": x.get("prop_integer"),
            }
        ).items()
        if v is not None
    }


class Pathological_MethodInputObj_Integer(TypedDict):
    prop_integer: NotRequired[int | None]


def encode_Pathological_MethodInputObj_Null(
    x: "Pathological_MethodInputObj_Null",
) -> Any:
    return {
        k: v
        for (k, v) in (
            {
                "prop_null": x.get("prop_null"),
            }
        ).items()
        if v is not None
    }


class Pathological_MethodInputObj_Null(TypedDict):
    prop_null: NotRequired[None]


def encode_Pathological_MethodInputObj_Number(
    x: "Pathological_MethodInputObj_Number",
) -> Any:
    return {
        k: v
        for (k, v) in (
            {
                "prop_number": x.get("prop_number"),
            }
        ).items()
        if v is not None
    }


class Pathological_MethodInputObj_Number(TypedDict):
    prop_number: NotRequired[float | None]


def encode_Pathological_MethodInputObj_String(
    x: "Pathological_MethodInputObj_String",
) -> Any:
    return {
        k: v
        for (k, v) in (
            {
                "prop_string": x.get("prop_string"),
            }
        ).items()
        if v is not None
    }


class Pathological_MethodInputObj_String(TypedDict):
    prop_string: NotRequired[str | None]


def encode_Pathological_MethodInputObj_Undefined(
    x: "Pathological_MethodInputObj_Undefined",
) -> Any:
    return {
        k: v
        for (k, v) in (
            {
                "prop_undefined": x.get("prop_undefined"),
            }
        ).items()
        if v is not None
    }


class Pathological_MethodInputObj_Undefined(TypedDict):
    prop_undefined: NotRequired[None]


def encode_Pathological_MethodInputReq_Obj_Date(
    x: "Pathological_MethodInputReq_Obj_Date",
) -> Any:
    return {
        k: v
        for (k, v) in (
            {
                "prop_Date": x.get("prop_Date"),
            }
        ).items()
        if v is not None
    }


class Pathological_MethodInputReq_Obj_Date(TypedDict):
    prop_Date: NotRequired[datetime.datetime | None]


def encode_Pathological_MethodInputReq_Obj_Uint8Array(
    x: "Pathological_MethodInputReq_Obj_Uint8Array",
) -> Any:
    return {
        k: v
        for (k, v) in (
            {
                "prop_Uint8Array": x.get("prop_Uint8Array"),
            }
        ).items()
        if v is not None
    }


class Pathological_MethodInputReq_Obj_Uint8Array(TypedDict):
    prop_Uint8Array: NotRequired[bytes | None]


def encode_Pathological_MethodInputReq_Obj_Boolean(
    x: "Pathological_MethodInputReq_Obj_Boolean",
) -> Any:
    return {
        k: v
        for (k, v) in (
            {
                "prop_boolean": x.get("prop_boolean"),
            }
        ).items()
        if v is not None
    }


class Pathological_MethodInputReq_Obj_Boolean(TypedDict):
    prop_boolean: NotRequired[bool | None]


def encode_Pathological_MethodInputReq_Obj_Integer(
    x: "Pathological_MethodInputReq_Obj_Integer",
) -> Any:
    return {
        k: v
        for (k, v) in (
            {
                "prop_integer": x.get("prop_integer"),
            }
        ).items()
        if v is not None
    }


class Pathological_MethodInputReq_Obj_Integer(TypedDict):
    prop_integer: NotRequired[int | None]


def encode_Pathological_MethodInputReq_Obj_Null(
    x: "Pathological_MethodInputReq_Obj_Null",
) -> Any:
    return {
        k: v
        for (k, v) in (
            {
                "prop_null": x.get("prop_null"),
            }
        ).items()
        if v is not None
    }


class Pathological_MethodInputReq_Obj_Null(TypedDict):
    prop_null: NotRequired[None]


def encode_Pathological_MethodInputReq_Obj_Number(
    x: "Pathological_MethodInputReq_Obj_Number",
) -> Any:
    return {
        k: v
        for (k, v) in (
            {
                "prop_number": x.get("prop_number"),
            }
        ).items()
        if v is not None
    }


class Pathological_MethodInputReq_Obj_Number(TypedDict):
    prop_number: NotRequired[float | None]


def encode_Pathological_MethodInputReq_Obj_String(
    x: "Pathological_MethodInputReq_Obj_String",
) -> Any:
    return {
        k: v
        for (k, v) in (
            {
                "prop_string": x.get("prop_string"),
            }
        ).items()
        if v is not None
    }


class Pathological_MethodInputReq_Obj_String(TypedDict):
    prop_string: NotRequired[str | None]


def encode_Pathological_MethodInputReq_Obj_Undefined(
    x: "Pathological_MethodInputReq_Obj_Undefined",
) -> Any:
    return {
        k: v
        for (k, v) in (
            {
                "prop_undefined": x.get("prop_undefined"),
            }
        ).items()
        if v is not None
    }


class Pathological_MethodInputReq_Obj_Undefined(TypedDict):
    prop_undefined: NotRequired[None]


def encode_Pathological_MethodInput(
    x: "Pathological_MethodInput",
) -> Any:
    return {
        k: v
        for (k, v) in (
            {
                "Date": x.get("Date"),
                "Uint8Array": x.get("Uint8Array"),
                "arr_Date": x.get("arr_Date"),
                "arr_Uint8Array": x.get("arr_Uint8Array"),
                "arr_boolean": x.get("arr_boolean"),
                "arr_integer": x.get("arr_integer"),
                "arr_null": x.get("arr_null"),
                "arr_number": x.get("arr_number"),
                "arr_string": x.get("arr_string"),
                "arr_undefined": x.get("arr_undefined"),
                "boolean": x.get("boolean"),
                "integer": x.get("integer"),
                "null": x.get("null"),
                "number": x.get("number"),
                "obj_Date": encode_Pathological_MethodInputObj_Date(x["obj_Date"])
                if "obj_Date" in x and x["obj_Date"] is not None
                else None,
                "obj_Uint8Array": encode_Pathological_MethodInputObj_Uint8Array(
                    x["obj_Uint8Array"]
                )
                if "obj_Uint8Array" in x and x["obj_Uint8Array"] is not None
                else None,
                "obj_boolean": encode_Pathological_MethodInputObj_Boolean(
                    x["obj_boolean"]
                )
                if "obj_boolean" in x and x["obj_boolean"] is not None
                else None,
                "obj_integer": encode_Pathological_MethodInputObj_Integer(
                    x["obj_integer"]
                )
                if "obj_integer" in x and x["obj_integer"] is not None
                else None,
                "obj_null": encode_Pathological_MethodInputObj_Null(x["obj_null"])
                if "obj_null" in x and x["obj_null"] is not None
                else None,
                "obj_number": encode_Pathological_MethodInputObj_Number(x["obj_number"])
                if "obj_number" in x and x["obj_number"] is not None
                else None,
                "obj_string": encode_Pathological_MethodInputObj_String(x["obj_string"])
                if "obj_string" in x and x["obj_string"] is not None
                else None,
                "obj_undefined": encode_Pathological_MethodInputObj_Undefined(
                    x["obj_undefined"]
                )
                if "obj_undefined" in x and x["obj_undefined"] is not None
                else None,
                "req_Date": x.get("req_Date"),
                "req_Uint8Array": x.get("req_Uint8Array"),
                "req_arr_Date": x.get("req_arr_Date"),
                "req_arr_Uint8Array": x.get("req_arr_Uint8Array"),
                "req_arr_boolean": x.get("req_arr_boolean"),
                "req_arr_integer": x.get("req_arr_integer"),
                "req_arr_null": x.get("req_arr_null"),
                "req_arr_number": x.get("req_arr_number"),
                "req_arr_string": x.get("req_arr_string"),
                "req_arr_undefined": x.get("req_arr_undefined"),
                "req_boolean": x.get("req_boolean"),
                "req_integer": x.get("req_integer"),
                "req_null": x.get("req_null"),
                "req_number": x.get("req_number"),
                "req_obj_Date": encode_Pathological_MethodInputReq_Obj_Date(
                    x["req_obj_Date"]
                )
                if "req_obj_Date" in x and x["req_obj_Date"] is not None
                else None,
                "req_obj_Uint8Array": encode_Pathological_MethodInputReq_Obj_Uint8Array(
                    x["req_obj_Uint8Array"]
                )
                if "req_obj_Uint8Array" in x and x["req_obj_Uint8Array"] is not None
                else None,
                "req_obj_boolean": encode_Pathological_MethodInputReq_Obj_Boolean(
                    x["req_obj_boolean"]
                )
                if "req_obj_boolean" in x and x["req_obj_boolean"] is not None
                else None,
                "req_obj_integer": encode_Pathological_MethodInputReq_Obj_Integer(
                    x["req_obj_integer"]
                )
                if "req_obj_integer" in x and x["req_obj_integer"] is not None
                else None,
                "req_obj_null": encode_Pathological_MethodInputReq_Obj_Null(
                    x["req_obj_null"]
                )
                if "req_obj_null" in x and x["req_obj_null"] is not None
                else None,
                "req_obj_number": encode_Pathological_MethodInputReq_Obj_Number(
                    x["req_obj_number"]
                )
                if "req_obj_number" in x and x["req_obj_number"] is not None
                else None,
                "req_obj_string": encode_Pathological_MethodInputReq_Obj_String(
                    x["req_obj_string"]
                )
                if "req_obj_string" in x and x["req_obj_string"] is not None
                else None,
                "req_obj_undefined": encode_Pathological_MethodInputReq_Obj_Undefined(
                    x["req_obj_undefined"]
                )
                if "req_obj_undefined" in x and x["req_obj_undefined"] is not None
                else None,
                "req_string": x.get("req_string"),
                "req_undefined": x.get("req_undefined"),
                "string": x.get("string"),
                "undefined": x.get("undefined"),
            }
        ).items()
        if v is not None
    }


class Pathological_MethodInput(TypedDict):
    Date: NotRequired[datetime.datetime | None]
    Uint8Array: NotRequired[bytes | None]
    arr_Date: NotRequired[list[datetime.datetime] | None]
    arr_Uint8Array: NotRequired[list[bytes] | None]
    arr_boolean: NotRequired[list[bool] | None]
    arr_integer: NotRequired[list[int] | None]
    arr_null: NotRequired[list[None] | None]
    arr_number: NotRequired[list[float] | None]
    arr_string: NotRequired[list[str] | None]
    arr_undefined: NotRequired[list[None] | None]
    boolean: NotRequired[bool | None]
    integer: NotRequired[int | None]
    null: NotRequired[None]
    number: NotRequired[float | None]
    obj_Date: NotRequired[Pathological_MethodInputObj_Date | None]
    obj_Uint8Array: NotRequired[Pathological_MethodInputObj_Uint8Array | None]
    obj_boolean: NotRequired[Pathological_MethodInputObj_Boolean | None]
    obj_integer: NotRequired[Pathological_MethodInputObj_Integer | None]
    obj_null: NotRequired[Pathological_MethodInputObj_Null | None]
    obj_number: NotRequired[Pathological_MethodInputObj_Number | None]
    obj_string: NotRequired[Pathological_MethodInputObj_String | None]
    obj_undefined: NotRequired[Pathological_MethodInputObj_Undefined | None]
    req_Date: datetime.datetime
    req_Uint8Array: bytes
    req_arr_Date: list[datetime.datetime]
    req_arr_Uint8Array: list[bytes]
    req_arr_boolean: list[bool]
    req_arr_integer: list[int]
    req_arr_null: list[None]
    req_arr_number: list[float]
    req_arr_string: list[str]
    req_arr_undefined: list[None]
    req_boolean: bool
    req_integer: int
    req_null: None
    req_number: float
    req_obj_Date: Pathological_MethodInputReq_Obj_Date
    req_obj_Uint8Array: Pathological_MethodInputReq_Obj_Uint8Array
    req_obj_boolean: Pathological_MethodInputReq_Obj_Boolean
    req_obj_integer: Pathological_MethodInputReq_Obj_Integer
    req_obj_null: Pathological_MethodInputReq_Obj_Null
    req_obj_number: Pathological_MethodInputReq_Obj_Number
    req_obj_string: Pathological_MethodInputReq_Obj_String
    req_obj_undefined: Pathological_MethodInputReq_Obj_Undefined
    req_string: str
    req_undefined: None
    string: NotRequired[str | None]
    undefined: NotRequired[None]
