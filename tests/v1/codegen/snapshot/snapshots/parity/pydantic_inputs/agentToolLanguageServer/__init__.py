# Code generated by river.codegen. DO NOT EDIT.
from collections.abc import AsyncIterable, AsyncIterator
from typing import Any
import datetime

from pydantic import TypeAdapter

from replit_river.error_schema import RiverError, RiverErrorTypeAdapter
import replit_river as river


from .openDocument import (
    OpendocumentErrors,
    OpendocumentErrorsTypeAdapter,
    OpendocumentInput,
    OpendocumentInputTypeAdapter,
    OpendocumentOutput,
    OpendocumentOutputTypeAdapter,
)


class AgenttoollanguageserverService:
    def __init__(self, client: river.Client[Any]):
        self.client = client

    async def openDocument(
        self,
        input: OpendocumentInput,
        timeout: datetime.timedelta,
    ) -> OpendocumentOutput:
        return await self.client.send_rpc(
            "agentToolLanguageServer",
            "openDocument",
            input,
            lambda x: OpendocumentInputTypeAdapter.dump_python(
                x,  # type: ignore[arg-type]
                by_alias=True,
                exclude_none=True,
            ),
            lambda x: OpendocumentOutputTypeAdapter.validate_python(
                x  # type: ignore[arg-type]
            ),
            lambda x: OpendocumentErrorsTypeAdapter.validate_python(
                x  # type: ignore[arg-type]
            ),
            timeout,
        )
