"""Tests the codegenerated River client based on the test proto, against the test server
built from that test proto."""

import os
import os.path
import subprocess
from typing import Optional

from websockets.client import connect

import river

from .client import test_river


def get_websocket_uri(
    host: str,
    endpoint: str = "",
    secure: Optional[bool] = None,
) -> str:
    """Returns a websocket URI"""
    if secure is None:
        secure = "localhost" not in host

    endpoint = endpoint.lstrip("/")

    if secure:
        return f"wss://{host}/{endpoint}"
    return f"ws://{host}/{endpoint}"


async def test_server() -> None:
    server_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "server")

    subprocess.check_call(["npm", "i"], cwd=server_dir)
    print("built the server")

    with subprocess.Popen(
        ["pnpm", "run", "--silent", "test", "50053"],
        cwd=server_dir,
        stdout=subprocess.PIPE,
    ) as proc:
        print("server started")
        # Wait for the River server to be up.

        if not proc or not proc.stdout:
            raise Exception("Server failed to start")

        proc.stdout.readline()

        try:
            HOST = "localhost:50053"
            async with connect(get_websocket_uri(host=HOST)) as websocket:
                print("Connected to WS")
                client = test_river.TestClient(river.Client(websocket))
                print("Built client")

                response = await client.test.echo(test_river.TestEchoInput(s="hello"))
                assert response.s == "hello", "Expected 'hello'"

        finally:
            proc.terminate()
            print("Server stopped")
