import asyncio
import random
from contextvars import Context
from typing import Dict

from replit_river.transport_options import ConnectionRetryOptions


class LeakyBucketRateLimit:
    """Asynchronous leaky bucket rate limiter.

    This class implements a rate limiting strategy using a leaky bucket algorithm,
    utilizing asyncio
    to handle periodic budget restoration in an asynchronous context.

    Attributes:
        options (ConnectionRetryOptions): Configuration options for retry behavior.
        budget_consumed (Dict[str, int]): Dictionary tracking the number of retries
        (or budget) consumed per user.
        tasks (Dict[str, asyncio.Task]): Dictionary holding asyncio tasks for budget
        restoration.
    """

    def __init__(self, options: ConnectionRetryOptions):
        self.options = options
        self.budget_consumed: Dict[str, int] = {}
        self.tasks: Dict[str, asyncio.Task] = {}

    def get_backoff_ms(self, user: str) -> float:
        """Calculate the backoff time in milliseconds for a user.

        Args:
            user (str): The identifier for the user.

        Returns:
            int: The backoff time in milliseconds, including a random jitter.
        """
        exponent = max(0, self.get_budget_consumed(user) - 1)
        jitter = random.randint(0, self.options.max_jitter_ms)
        backoff_ms = min(
            float(self.options.base_interval_ms * (2**exponent)),
            float(self.options.max_backoff_ms),
        )
        return backoff_ms + jitter

    def get_budget_consumed(self, user: str) -> int:
        """Retrieve the amount of budget consumed for the specified user.

        Args:
            user (str): The identifier for the user.

        Returns:
            int: The number of times the budget has been consumed.
        """
        return self.budget_consumed.get(user, 0)

    def has_budget(self, user: str) -> bool:
        """Check if the user has remaining budget to make a retry.

        Args:
            user (str): The identifier for the user.

        Returns:
            bool: True if budget is available, False otherwise.
        """
        return self.get_budget_consumed(user) < self.options.attempt_budget_capacity

    def consume_budget(self, user: str) -> None:
        """Increment the budget consumed for the user by 1, indicating a retry attempt.

        Args:
            user (str): The identifier for the user.
        """
        if user in self.tasks:
            self.tasks[user].cancel()
            del self.tasks[user]
        current_budget = self.get_budget_consumed(user)
        self.budget_consumed[user] = current_budget + 1

    def start_restoring_budget(self, user: str) -> None:
        """Start or reset an asynchronous task to restore budget periodically for the
        user.

        Args:
            user (str): The identifier for the user.
        """
        self.tasks[user] = asyncio.create_task(
            self.restore_budget(user), context=Context()
        )

    async def restore_budget(self, user: str) -> None:
        """Asynchronously wait for the interval and then restore the budget for the
        user.

        Args:
            user (str): The identifier for the user.
        """
        while self.budget_consumed.get(user, 0) > 0:
            await asyncio.sleep(self.options.budget_restore_interval_ms / 1000.0)
            if self.budget_consumed[user] == 0:
                break
            self.budget_consumed[user] -= 1

    def close(self) -> None:
        """Cancel all asynchronous tasks when closing the limiter."""
        for task in self.tasks.values():
            task.cancel()
        self.tasks.clear()
