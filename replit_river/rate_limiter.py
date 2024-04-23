import random
from threading import Timer

from replit_river.transport_options import ConnectionRetryOptions


class LeakyBucketRateLimit:

    def __init__(self, options: ConnectionRetryOptions):
        self.options = options
        self.budget_consumed = {}
        self.interval_handles = {}

    def get_backoff_ms(self, user):
        exponent = max(0, self.get_budget_consumed(user) - 1)
        jitter = random.randint(0, self.options.max_jitter_ms)
        backoff_ms = min(
            self.options.base_interval_ms * (2**exponent), self.options.max_backoff_ms
        )
        return backoff_ms + jitter

    def get_budget_consumed(self, user):
        return self.budget_consumed.get(user, 0)

    def has_budget(self, user):
        return self.get_budget_consumed(user) < self.options.attempt_budget_capacity

    def consume_budget(self, user):
        self.stop_leak(user)
        self.budget_consumed[user] = self.get_budget_consumed(user) + 1

    def start_restoring_budget(self, user):
        if user in self.interval_handles:
            return

        def restore_budget_for_user():
            current_budget = self.get_budget_consumed(user)
            if current_budget == 0:
                self.stop_leak(user)
            else:
                self.budget_consumed[user] = max(current_budget - 1, 0)

        interval_handle = Timer(
            self.options.budget_restore_interval_ms / 1000.0, restore_budget_for_user
        )
        interval_handle.start()
        self.interval_handles[user] = interval_handle

    def stop_leak(self, user):
        if user in self.interval_handles:
            self.interval_handles[user].cancel()
            del self.interval_handles[user]

    def close(self):
        for user in list(self.interval_handles.keys()):
            self.stop_leak(user)
