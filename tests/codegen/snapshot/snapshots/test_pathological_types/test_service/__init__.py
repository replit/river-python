# Code generated by river.codegen. DO NOT EDIT.
from collections.abc import AsyncIterable, AsyncIterator
from typing import Any
import datetime

from pydantic import TypeAdapter

from replit_river.error_schema import RiverError, RiverErrorTypeAdapter
import replit_river as river


from .pathological_method import (
    Pathological_MethodInput,
    encode_Pathological_MethodInput,
    encode_Pathological_MethodInputObj_Boolean,
    encode_Pathological_MethodInputObj_Date,
    encode_Pathological_MethodInputObj_Integer,
    encode_Pathological_MethodInputObj_Null,
    encode_Pathological_MethodInputObj_Number,
    encode_Pathological_MethodInputObj_String,
    encode_Pathological_MethodInputObj_Uint8Array,
    encode_Pathological_MethodInputObj_Undefined,
    encode_Pathological_MethodInputReq_Obj_Boolean,
    encode_Pathological_MethodInputReq_Obj_Date,
    encode_Pathological_MethodInputReq_Obj_Integer,
    encode_Pathological_MethodInputReq_Obj_Null,
    encode_Pathological_MethodInputReq_Obj_Number,
    encode_Pathological_MethodInputReq_Obj_String,
    encode_Pathological_MethodInputReq_Obj_Uint8Array,
    encode_Pathological_MethodInputReq_Obj_Undefined,
)

boolTypeAdapter: TypeAdapter[bool] = TypeAdapter(bool)


class Test_ServiceService:
    def __init__(self, client: river.Client[Any]):
        self.client = client

    async def pathological_method(
        self,
        input: Pathological_MethodInput,
        timeout: datetime.timedelta,
    ) -> bool:
        return await self.client.send_rpc(
            "test_service",
            "pathological_method",
            input,
            encode_Pathological_MethodInput,
            lambda x: boolTypeAdapter.validate_python(
                x  # type: ignore[arg-type]
            ),
            lambda x: RiverErrorTypeAdapter.validate_python(
                x  # type: ignore[arg-type]
            ),
            timeout,
        )
