# Code generated by river.codegen. DO NOT EDIT.
from pydantic import BaseModel
from typing import Literal

import replit_river as river


from .test_service import Test_ServiceService


class RpcClient:
    def __init__(self, client: river.Client[Literal[None]]):
        self.test_service = Test_ServiceService(client)
