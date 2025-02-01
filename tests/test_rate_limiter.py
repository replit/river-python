import asyncio

import pytest

from replit_river.rate_limiter import LeakyBucketRateLimit
from replit_river.transport_options import ConnectionRetryOptions


@pytest.fixture
def options() -> ConnectionRetryOptions:
    return ConnectionRetryOptions(
        base_interval_ms=100,
        max_jitter_ms=50,
        max_backoff_ms=1000,
        attempt_budget_capacity=5,
        budget_restore_interval_ms=200,
    )


@pytest.fixture
def rate_limiter(options: ConnectionRetryOptions) -> LeakyBucketRateLimit:
    return LeakyBucketRateLimit(options)


@pytest.mark.asyncio
async def test_initial_budget(rate_limiter: LeakyBucketRateLimit) -> None:
    user: str = "user1"
    assert rate_limiter.has_budget(user), "User should initially have full budget"


@pytest.mark.asyncio
async def test_consume_budget(rate_limiter: LeakyBucketRateLimit) -> None:
    user: str = "user2"
    rate_limiter.consume_budget(user)
    assert rate_limiter.get_budget_consumed(user) == 1, (
        "Budget consumed should be incremented"
    )


@pytest.mark.asyncio
async def test_restore_budget(rate_limiter: LeakyBucketRateLimit) -> None:
    user: str = "user3"
    rate_limiter.consume_budget(user)
    rate_limiter.start_restoring_budget(user)
    await asyncio.sleep(0.3)  # Wait more than budget restore interval
    assert rate_limiter.get_budget_consumed(user) == 0, (
        "Budget should be restored after interval"
    )


@pytest.mark.asyncio
async def test_concurrent_access(rate_limiter: LeakyBucketRateLimit) -> None:
    user: str = "user4"

    async def consume_budget() -> None:
        for _ in range(5):
            rate_limiter.consume_budget(user)
            await asyncio.sleep(0.01)  # simulate some delay

    await asyncio.gather(consume_budget(), consume_budget())
    assert rate_limiter.get_budget_consumed(user) == 10, (
        "Concurrent access should be handled correctly"
    )


def test_close(rate_limiter: LeakyBucketRateLimit) -> None:
    rate_limiter.close()
    assert not rate_limiter.tasks, "All tasks should be cancelled upon close"
