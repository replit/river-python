import asyncio
from contextlib import asynccontextmanager
from typing import AsyncGenerator, ParamSpec, TypeVar

P = ParamSpec("P")
R = TypeVar("R")


class AcquiredLock:
    def __init__(self, lock: asyncio.Lock):
        self._lock = lock


class TransferableLock:
    """A lock that can be transferred between coroutines."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()

        class BoundLock(AcquiredLock):
            pass

        self.BoundLock = BoundLock

    @asynccontextmanager
    async def __call__(self) -> AsyncGenerator[AcquiredLock]:
        await self._lock.acquire()
        yield self.BoundLock(self._lock)
        self._lock.release()


def assert_correct_lock(
    acquired_lock: AcquiredLock, expected_lock_class: TransferableLock
) -> None:
    """Assert that the acquired lock is the correct class."""
    if not isinstance(acquired_lock, expected_lock_class.BoundLock):
        raise ValueError(
            f"Expected {expected_lock_class.BoundLock.__name__}, "
            f"got {type(acquired_lock).__name__}"
        )
