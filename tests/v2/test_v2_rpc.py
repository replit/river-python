import importlib
import logging
from collections import deque
from datetime import timedelta
from typing import (
    Literal,
)

import pytest
from pytest_snapshot.plugin import Snapshot

from replit_river.v2.client import Client
from tests.fixtures.codegen_snapshot_fixtures import validate_codegen
from tests.v2.datagrams import (
    ClientId,
    FromClient,
    ServerId,
    StreamAlias,
    TestTransport,
    ToClient,
    ValueSet,
    WaitForClosed,
)

logger = logging.getLogger(__name__)

_AlreadyGenerated = False


@pytest.fixture(scope="function", autouse=True)
def stream_client_codegen(snapshot: Snapshot) -> Literal[True]:
    global _AlreadyGenerated
    if not _AlreadyGenerated:
        validate_codegen(
            snapshot=snapshot,
            snapshot_dir="tests/v2/codegen/snapshot/snapshots",
            read_schema=lambda: open("tests/v2/test_rpc.schema.json"),
            target_path="test_basic_rpc",
            client_name="StreamClient",
            protocol_version="v2.0",
        )
        _AlreadyGenerated = True

    import tests.v2.codegen.snapshot.snapshots.test_basic_stream

    importlib.reload(tests.v2.codegen.snapshot.snapshots.test_basic_stream)
    return True


rpc_expected: deque[TestTransport] = deque(
    [
        FromClient(
            handshake_request=ValueSet(
                seq=0,  # These don't count due to being during a handshake
                ack=0,
                from_=ServerId("server-001"),
                to=ClientId("client-001"),
            )
        ),
        FromClient(
            stream_open=ValueSet(
                seq=0,
                ack=0,
                from_=ServerId("server-001"),
                to=ClientId("client-001"),
                serviceName="test_service",
                procedureName="rpc_method",
                create_alias=StreamAlias(1),
                payload={"data": "foo"},
                stream_closed=True,
            )
        ),
        ToClient(
            seq=0,
            ack=1,
            stream_frame=(
                StreamAlias(1),
                {"ok": True, "payload": {"data": "Hello, foo!"}},
            ),
        ),
        WaitForClosed(),
    ]
)


@pytest.mark.parametrize("expected", [rpc_expected])
async def test_rpc(bound_client: Client) -> None:
    from tests.v2.codegen.snapshot.snapshots.test_basic_rpc import (
        StreamClient,
    )

    res = await StreamClient(bound_client).test_service.rpc_method(
        init={"data": "foo"},
        timeout=timedelta(seconds=5),
    )

    assert res.data == "Hello, foo!"
