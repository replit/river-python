# Code generated by river.codegen. DO NOT EDIT.
from collections.abc import AsyncIterable, AsyncIterator
from typing import Any
import datetime

from pydantic import TypeAdapter

from replit_river.error_schema import RiverError
import replit_river as river


from .needsEnum import (
    NeedsenumErrors,
    encode_NeedsenumInput,
    NeedsenumInput,
    NeedsenumOutput,
)
from .needsEnumObject import (
    encode_NeedsenumobjectInput,
    NeedsenumobjectOutput,
    NeedsenumobjectInput,
    NeedsenumobjectErrors,
)


class EnumserviceService:
    def __init__(self, client: river.Client[Any]):
        self.client = client

    async def needsEnum(
        self,
        input: NeedsenumInput,
        timeout: datetime.timedelta,
    ) -> NeedsenumOutput:
        return await self.client.send_rpc(
            "enumService",
            "needsEnum",
            input,
            lambda x: x,
            lambda x: TypeAdapter(NeedsenumOutput).validate_python(
                x  # type: ignore[arg-type]
            ),
            lambda x: TypeAdapter(NeedsenumErrors).validate_python(
                x  # type: ignore[arg-type]
            ),
            timeout,
        )

    async def needsEnumObject(
        self,
        input: NeedsenumobjectInput,
        timeout: datetime.timedelta,
    ) -> NeedsenumobjectOutput:
        return await self.client.send_rpc(
            "enumService",
            "needsEnumObject",
            input,
            encode_NeedsenumobjectInput,
            lambda x: TypeAdapter(NeedsenumobjectOutput).validate_python(
                x  # type: ignore[arg-type]
            ),
            lambda x: TypeAdapter(NeedsenumobjectErrors).validate_python(
                x  # type: ignore[arg-type]
            ),
            timeout,
        )
