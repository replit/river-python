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


def encode_InitInit(
    _: "InitInit",
) -> Any:
    return {}


class InitInit(TypedDict):
    pass


def encode_InitInputOneOf_closeFile(
    x: "InitInputOneOf_closeFile",
) -> Any:
    return {
        k: v
        for (k, v) in (
            {
                "$kind": x.get("kind"),
                "filename": x.get("filename"),
                "nonce": x.get("nonce"),
            }
        ).items()
        if v is not None
    }


class InitInputOneOf_closeFile(TypedDict):
    kind: Annotated[Literal["closeFile"], Field(alias="$kind")]
    filename: str
    nonce: str


def encode_InitInputOneOf_githubToken(
    x: "InitInputOneOf_githubToken",
) -> Any:
    return {
        k: v
        for (k, v) in (
            {
                "$kind": x.get("kind"),
                "nonce": x.get("nonce"),
                "token": x.get("token"),
            }
        ).items()
        if v is not None
    }


class InitInputOneOf_githubToken(TypedDict):
    kind: Annotated[Literal["githubToken"], Field(alias="$kind")]
    nonce: str
    token: NotRequired[str | None]


def encode_InitInputOneOf_sshToken0(
    x: "InitInputOneOf_sshToken0",
) -> Any:
    return {
        k: v
        for (k, v) in (
            {
                "$kind": x.get("kind"),
                "SSHHostname": x.get("SSHHostname"),
                "nonce": x.get("nonce"),
                "token": x.get("token"),
            }
        ).items()
        if v is not None
    }


class InitInputOneOf_sshToken0(TypedDict):
    kind: Annotated[Literal["sshToken"], Field(alias="$kind")]
    SSHHostname: str
    nonce: str
    token: str


def encode_InitInputOneOf_sshToken1(
    x: "InitInputOneOf_sshToken1",
) -> Any:
    return {
        k: v
        for (k, v) in (
            {
                "$kind": x.get("kind"),
                "error": x.get("error"),
                "nonce": x.get("nonce"),
            }
        ).items()
        if v is not None
    }


class InitInputOneOf_sshToken1(TypedDict):
    kind: Annotated[Literal["sshToken"], Field(alias="$kind")]
    error: str
    nonce: str


def encode_InitInputOneOf_allowDefaultBucketAccessResultOneOf_error(
    x: "InitInputOneOf_allowDefaultBucketAccessResultOneOf_error",
) -> Any:
    return {
        k: v
        for (k, v) in (
            {
                "message": x.get("message"),
                "status": x.get("status"),
            }
        ).items()
        if v is not None
    }


class InitInputOneOf_allowDefaultBucketAccessResultOneOf_error(TypedDict):
    message: str
    status: Literal["error"]


def encode_InitInputOneOf_allowDefaultBucketAccessResultOneOf_ok(
    x: "InitInputOneOf_allowDefaultBucketAccessResultOneOf_ok",
) -> Any:
    return {
        k: v
        for (k, v) in (
            {
                "bucketId": x.get("bucketId"),
                "sourceReplId": x.get("sourceReplId"),
                "status": x.get("status"),
                "targetReplId": x.get("targetReplId"),
            }
        ).items()
        if v is not None
    }


class InitInputOneOf_allowDefaultBucketAccessResultOneOf_ok(TypedDict):
    bucketId: str
    sourceReplId: str
    status: Literal["ok"]
    targetReplId: str


InitInputOneOf_allowDefaultBucketAccessResult = (
    InitInputOneOf_allowDefaultBucketAccessResultOneOf_error
    | InitInputOneOf_allowDefaultBucketAccessResultOneOf_ok
)


def encode_InitInputOneOf_allowDefaultBucketAccessResult(
    x: "InitInputOneOf_allowDefaultBucketAccessResult",
) -> Any:
    return (
        encode_InitInputOneOf_allowDefaultBucketAccessResultOneOf_error(x)
        if x["status"] == "error"
        else encode_InitInputOneOf_allowDefaultBucketAccessResultOneOf_ok(x)
    )


def encode_InitInputOneOf_allowDefaultBucketAccess(
    x: "InitInputOneOf_allowDefaultBucketAccess",
) -> Any:
    return {
        k: v
        for (k, v) in (
            {
                "$kind": x.get("kind"),
                "nonce": x.get("nonce"),
                "result": encode_InitInputOneOf_allowDefaultBucketAccessResult(
                    x["result"]
                ),
            }
        ).items()
        if v is not None
    }


class InitInputOneOf_allowDefaultBucketAccess(TypedDict):
    kind: Annotated[Literal["allowDefaultBucketAccess"], Field(alias="$kind")]
    nonce: str
    result: InitInputOneOf_allowDefaultBucketAccessResult


InitInput = (
    InitInputOneOf_closeFile
    | InitInputOneOf_githubToken
    | InitInputOneOf_sshToken0
    | InitInputOneOf_sshToken1
    | InitInputOneOf_allowDefaultBucketAccess
)


def encode_InitInput(
    x: "InitInput",
) -> Any:
    return (
        encode_InitInputOneOf_closeFile(x)
        if x["kind"] == "closeFile"
        else encode_InitInputOneOf_githubToken(x)
        if x["kind"] == "githubToken"
        else (
            encode_InitInputOneOf_sshToken0(x)  # type: ignore[arg-type]
            if "token" in x
            else encode_InitInputOneOf_sshToken1(x)  # type: ignore[arg-type]
        )
        if x["kind"] == "sshToken"
        else encode_InitInputOneOf_allowDefaultBucketAccess(x)
    )


class InitOutput(BaseModel):
    pass


InitOutputTypeAdapter: TypeAdapter[InitOutput] = TypeAdapter(InitOutput)


class InitErrors(RiverError):
    pass


InitErrorsTypeAdapter: TypeAdapter[InitErrors] = TypeAdapter(InitErrors)
