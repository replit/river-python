# Code generated by river.codegen. DO NOT EDIT.
from collections.abc import AsyncIterable, AsyncIterator
from typing import Any
import datetime

from pydantic import TypeAdapter

from replit_river.error_schema import RiverError, RiverErrorTypeAdapter
import replit_river as river


from .exec import (
    ExecErrors,
    ExecErrorsTypeAdapter,
    ExecInit,
    ExecInitTypeAdapter,
    ExecInput,
    ExecInputTypeAdapter,
    ExecOutput,
    ExecOutputTypeAdapter,
)


class AiexecService:
    def __init__(self, client: river.Client[Any]):
        self.client = client

    async def exec(
        self,
        init: ExecInit,
        inputStream: AsyncIterable[ExecInput],
    ) -> AsyncIterator[ExecOutput | ExecErrors | RiverError]:
        return self.client.send_stream(
            "aiExec",
            "exec",
            init,
            inputStream,
            lambda x: ExecInitTypeAdapter.validate_python(x),
            lambda x: ExecInputTypeAdapter.dump_python(
                x,  # type: ignore[arg-type]
                by_alias=True,
                exclude_none=True,
            ),
            lambda x: ExecOutputTypeAdapter.validate_python(
                x  # type: ignore[arg-type]
            ),
            lambda x: ExecErrorsTypeAdapter.validate_python(
                x  # type: ignore[arg-type]
            ),
        )
