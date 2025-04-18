# Code generated by river.codegen. DO NOT EDIT.
from collections.abc import AsyncIterable, AsyncIterator
from typing import Any
import datetime

from pydantic import TypeAdapter

from replit_river.error_schema import RiverError, RiverErrorTypeAdapter
import replit_river as river


from .spawn import (
    SpawnErrors,
    SpawnErrorsTypeAdapter,
    SpawnInput,
    SpawnInputTypeAdapter,
    SpawnOutput,
    SpawnOutputTypeAdapter,
)


class ShellexecService:
    def __init__(self, client: river.Client[Any]):
        self.client = client

    async def spawn(
        self,
        input: SpawnInput,
        timeout: datetime.timedelta,
    ) -> SpawnOutput:
        return await self.client.send_rpc(
            "shellExec",
            "spawn",
            input,
            lambda x: SpawnInputTypeAdapter.dump_python(
                x,  # type: ignore[arg-type]
                by_alias=True,
                exclude_none=True,
            ),
            lambda x: SpawnOutputTypeAdapter.validate_python(
                x  # type: ignore[arg-type]
            ),
            lambda x: SpawnErrorsTypeAdapter.validate_python(
                x  # type: ignore[arg-type]
            ),
            timeout,
        )
