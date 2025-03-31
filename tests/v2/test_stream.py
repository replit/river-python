import importlib
import logging
from collections import deque
from typing import (
    AsyncIterable,
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
            read_schema=lambda: open("tests/v2/test_stream.schema.json"),
            target_path="test_basic_stream",
            client_name="StreamClient",
            protocol_version="v2.0",
        )
        _AlreadyGenerated = True

    import tests.v2.codegen.snapshot.snapshots.test_basic_stream

    importlib.reload(tests.v2.codegen.snapshot.snapshots.test_basic_stream)
    return True


stream_expected: deque[TestTransport] = deque(
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
                procedureName="stream_method",
                create_alias=StreamAlias(1),
            )
        ),
        FromClient(
            stream_frame=ValueSet(
                seq=1,
                ack=0,
                stream_alias=StreamAlias(1),
                payload={"data": "0"},
            )
        ),
        ToClient(
            seq=0,
            ack=1,
            stream_frame=(
                StreamAlias(1),
                {"ok": True, "payload": {"data": "Stream response for 0"}},
            ),
        ),
        FromClient(
            stream_frame=ValueSet(
                seq=2,
                ack=0,
                stream_alias=StreamAlias(1),
                payload={"data": "1"},
            )
        ),
        ToClient(
            seq=1,
            ack=2,
            stream_frame=(
                StreamAlias(1),
                {"ok": True, "payload": {"data": "Stream response for 1"}},
            ),
        ),
        FromClient(
            stream_frame=ValueSet(
                seq=3,
                ack=0,
                stream_alias=StreamAlias(1),
                payload={"data": "2"},
            )
        ),
        ToClient(
            seq=2,
            ack=0,
            stream_frame=(
                StreamAlias(1),
                {"ok": True, "payload": {"data": "Stream response for 2"}},
            ),
        ),
        FromClient(
            stream_frame=ValueSet(
                seq=4,
                ack=0,
                stream_alias=StreamAlias(1),
                payload={"data": "3"},
            )
        ),
        ToClient(
            seq=3,
            ack=4,
            stream_frame=(
                StreamAlias(1),
                {"ok": True, "payload": {"data": "Stream response for 3"}},
            ),
        ),
        FromClient(
            stream_frame=ValueSet(
                seq=5,
                ack=0,
                stream_alias=StreamAlias(1),
                payload={"data": "4"},
            )
        ),
        ToClient(
            seq=4,
            ack=5,
            stream_frame=(
                StreamAlias(1),
                {"ok": True, "payload": {"data": "Stream response for 4"}},
            ),
        ),
        FromClient(
            stream_frame=ValueSet(
                seq=6,
                ack=0,
                stream_alias=StreamAlias(1),
                payload={"type": "CLOSE"},
            )
        ),
        ToClient(
            seq=5,
            ack=0,
            stream_close=StreamAlias(1),
        ),
        WaitForClosed(),
    ]
)


@pytest.mark.parametrize("expected", [stream_expected])
async def test_stream(bound_client: Client) -> None:
    from tests.v2.codegen.snapshot.snapshots.test_basic_stream import (
        StreamClient,
    )
    from tests.v2.codegen.snapshot.snapshots.test_basic_stream.test_service.stream_method import (  # noqa: E501
        Stream_MethodInput,
        Stream_MethodOutput,
    )

    async def emit() -> AsyncIterable[Stream_MethodInput]:
        for i in range(5):
            data: Stream_MethodInput = {"data": str(i)}
            yield data

    res = await StreamClient(bound_client).test_service.stream_method(
        init={},
        inputStream=emit(),
    )

    i = 0
    async for datum in res:
        assert isinstance(datum, Stream_MethodOutput)
        assert f"Stream response for {i}" == datum.data, f"{i} == {datum.data}"
        i = i + 1
    assert i == 5
