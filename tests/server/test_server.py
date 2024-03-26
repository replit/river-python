import asyncio
import os
import os.path
import subprocess
from typing import Any, AsyncIterator

from websockets.server import serve

import river

from .server import test_pb2, test_pb2_grpc, test_river


class TestServicer(test_pb2_grpc.TestServicer):
    async def rpc(self, msg: Any, context: Any) -> test_pb2.RpcResponse:
        return test_pb2.RpcResponse(payload=msg.string)

    async def stream(
        self, msgStream: Any, context: Any
    ) -> AsyncIterator[test_pb2.StreamResponse]:
        async for msg in msgStream:
            yield test_pb2.StreamResponse(payload=msg.payload)


async def test_server() -> None:
    print("started test")
    client_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "client")
    subprocess.check_call(["npm", "i"], cwd=client_dir)
    subprocess.check_call(["npm", "run", "build"], cwd=client_dir)
    print("built the client")

    test_servicer = TestServicer()
    server = river.Server()
    test_river.add_TestServicer_to_server(test_servicer, server)  # type: ignore
    print("Starting River Server")
    done: asyncio.Future[None] = asyncio.Future()
    started: asyncio.Future[None] = asyncio.Future()

    async def _serve() -> None:
        async with serve(server.serve, "127.0.0.1", 50052):
            started.set_result(None)
            print("server started")
            await done

    async def _run_test() -> None:
        await started
        try:
            print("going to run test")
            proc = await asyncio.create_subprocess_exec(
                "npm", "run", "test", cwd=client_dir
            )
            assert await proc.wait() == 0
        finally:
            done.set_result(None)

    async with asyncio.TaskGroup() as tg:
        tg.create_task(_serve())
        tg.create_task(_run_test())
