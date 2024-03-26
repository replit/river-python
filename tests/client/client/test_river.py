# Code generated by river.codegen. DO NOT EDIT.

import river
from pydantic import BaseModel, parse_obj_as
from river.error_schema import RiverError


class TestEchoInput(BaseModel):
    s: str


class TestEchoOutput(BaseModel):
    s: str


class TestService:
    def __init__(self, client: river.Client):
        self.client = client

    async def echo(
        self,
        input: TestEchoInput,
    ) -> TestEchoOutput:
        return await self.client.send_rpc(
            "test",
            "echo",
            input,
            lambda x: x.model_dump(by_alias=True),
            lambda x: parse_obj_as(TestEchoOutput, x),  # type: ignore[arg-type]
            lambda x: parse_obj_as(RiverError, x),  # type: ignore[arg-type]
        )


class TestClient:
    def __init__(self, client: river.Client):
        self.test = TestService(client)
