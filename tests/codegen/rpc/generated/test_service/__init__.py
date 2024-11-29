# Code generated by river.codegen. DO NOT EDIT.
from collections.abc import AsyncIterable, AsyncIterator
from typing import Any
import datetime

from pydantic import TypeAdapter

from replit_river.error_schema import RiverError
import replit_river as river


from .rpc_method import encode_Rpc_MethodInput, Rpc_MethodInput, Rpc_MethodOutput


class Test_ServiceService:
    def __init__(self, client: river.Client[Any]):
        self.client = client

    async def rpc_method(
        self,
        input: Rpc_MethodInput,
        timeout: datetime.timedelta,
    ) -> Rpc_MethodOutput:
        return await self.client.send_rpc(
            "test_service",
            "rpc_method",
            input,
            encode_Rpc_MethodInput,
            lambda x: TypeAdapter(Rpc_MethodOutput).validate_python(
                x  # type: ignore[arg-type]
            ),
            lambda x: TypeAdapter(RiverError).validate_python(
                x  # type: ignore[arg-type]
            ),
            timeout,
        )