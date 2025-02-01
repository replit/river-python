import asyncio

import pytest

from replit_river.seq_manager import (
    IgnoreMessageException,
    OutOfOrderMessageException,
    SeqManager,
)
from tests.conftest import transport_message
from tests.river_fixtures.logging import NoErrors


@pytest.mark.asyncio
async def test_initial_sequence_and_ack_numbers(no_logging_error: NoErrors) -> None:
    manager = SeqManager()
    assert await manager.get_seq() == 0, "Initial sequence number should be 0"
    assert await manager.get_ack() == 0, "Initial acknowledgment number should be 0"
    no_logging_error()


@pytest.mark.asyncio
async def test_sequence_number_increment(no_logging_error: NoErrors) -> None:
    manager = SeqManager()
    initial_seq = await manager.get_seq_and_increment()
    assert initial_seq == 0, "Sequence number should start at 0"
    new_seq = await manager.get_seq()
    assert new_seq == 1, "Sequence number should increment to 1"
    no_logging_error()


@pytest.mark.asyncio
async def test_message_reception(no_logging_error: NoErrors) -> None:
    manager = SeqManager()
    msg = transport_message(seq=0, ack=0, from_="client")
    await manager.check_seq_and_update(
        msg
    )  # No error should be raised for the correct sequence
    assert await manager.get_ack() == 1, "Acknowledgment should be set to 1"

    # We assert no errors before we send out-of-order messages
    no_logging_error()

    # Test duplicate message
    with pytest.raises(IgnoreMessageException):
        await manager.check_seq_and_update(msg)

    # Test out of order message
    msg.seq = 2
    with pytest.raises(OutOfOrderMessageException):
        await manager.check_seq_and_update(msg)


@pytest.mark.asyncio
async def test_acknowledgment_setting(no_logging_error: NoErrors) -> None:
    manager = SeqManager()
    msg = transport_message(seq=0, ack=0, from_="client")
    await manager.check_seq_and_update(msg)
    assert await manager.get_ack() == 1, "Acknowledgment number should be updated"
    no_logging_error()


@pytest.mark.asyncio
async def test_concurrent_access_to_sequence(no_logging_error: NoErrors) -> None:
    manager = SeqManager()
    tasks = [manager.get_seq_and_increment() for _ in range(10)]
    results = await asyncio.gather(*tasks)
    assert len(set(results)) == 10, (
        "Each increment call should return a unique sequence number"
    )
    assert await manager.get_seq() == 10, (
        "Final sequence number should be 10 after 10 increments"
    )
    no_logging_error()
