import asyncio

import pytest

from replit_river.message_buffer import MessageBuffer, MessageBufferClosedError
from replit_river.rpc import TransportMessage


def mock_transport_message(seq: int) -> TransportMessage:
    return TransportMessage(
        seq=seq,
        id="test",
        ack=0,
        from_="test",  # type: ignore
        to="test",
        streamId="test",
        controlFlags=0,
        payload=0,
        model_config={},  # type: ignore
    )


async def test_message_buffer_backpressure() -> None:
    """
    Tests that MessageBuffer.put blocks until there is space in the buffer,
    creating back pressure in the client.
    """
    buffer = MessageBuffer(max_num_messages=1)

    iterations = 100

    # We use a queue as a way to sync our test logic with the background
    # task with the testing logic.
    sync_events: asyncio.Queue[None] = asyncio.Queue()

    async def put_messages() -> None:
        for i in range(0, iterations):
            await buffer.put(mock_transport_message(seq=i))
            await sync_events.put(None)

    background_puts = asyncio.create_task(put_messages())

    for i in range(1, iterations):
        # Wait for the put call to return.
        await sync_events.get()
        assert len(buffer.buffer) == 1
        await buffer.remove_old_messages(i)

    await background_puts


async def test_message_buffer_close() -> None:
    """
    Tests that MessageBuffer.put raises an exception when the buffer
    is closed while the put operation is waiting for space in the buffer.
    """
    buffer = MessageBuffer(max_num_messages=1)
    await buffer.put(mock_transport_message(seq=1))
    background_put = asyncio.create_task(buffer.put(mock_transport_message(seq=1)))
    await buffer.close()
    with pytest.raises(MessageBufferClosedError):
        await background_put
    with pytest.raises(MessageBufferClosedError):
        await buffer.put(mock_transport_message(seq=1))
