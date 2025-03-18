import asyncio

import pytest

from replit_river.lock import AcquiredLock, TransferableLock, assert_correct_lock


async def test_basic_lock():
    lock = TransferableLock()
    acquired = False

    async with lock():
        acquired = True

    assert acquired


async def test_lock_transfer():
    lock = TransferableLock()
    results = []

    async def function_b(acquired_lock: AcquiredLock):
        assert_correct_lock(acquired_lock, lock)
        results.append("b acquired")
        await asyncio.sleep(0.1)  # Simulate work
        results.append("b released")

    async def function_a():
        async with lock() as acquired_lock:
            results.append("a acquired")
            await function_b(acquired_lock)
            results.append("a released")

    await function_a()

    assert results == ["a acquired", "b acquired", "b released", "a released"]


async def test_concurrent_access():
    lock = TransferableLock()
    results = []

    async def worker(acquired_lock: AcquiredLock, name: str):
        assert_correct_lock(acquired_lock, lock)
        results.append(f"{name} acquired")
        await asyncio.sleep(0.1)  # Simulate work

    # Run ten workers concurrently
    async with lock() as acquired_lock:
        await asyncio.gather(
            worker(acquired_lock, "worker1"),
            worker(acquired_lock, "worker2"),
            worker(acquired_lock, "worker3"),
            worker(acquired_lock, "worker4"),
            worker(acquired_lock, "worker5"),
            worker(acquired_lock, "worker6"),
            worker(acquired_lock, "worker7"),
            worker(acquired_lock, "worker8"),
            worker(acquired_lock, "worker9"),
            worker(acquired_lock, "worker10"),
        )

    assert len(results) == 10


async def test_label_mismatch():
    lock = TransferableLock()
    lock2 = TransferableLock()
    results = []

    async def function_b(acquired_lock: AcquiredLock):
        assert_correct_lock(acquired_lock, lock)
        results.append("b acquired")
        await asyncio.sleep(0.1)
        results.append("b released")

    async def function_a():
        async with lock2() as acquired_lock:
            results.append("a acquired")
            await function_b(acquired_lock)
            results.append("a released")

    with pytest.raises(ValueError):
        await function_a()
