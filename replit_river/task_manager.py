import asyncio
import logging
from typing import Any, Set


class BackgroundTaskManager:
    """Manages background tasks and logs exceptions."""

    def __init__(self) -> None:
        self.background_tasks: Set[asyncio.Task] = set()

    def cancel_all_tasks(self) -> None:
        for task in self.background_tasks:
            task.cancel()

    def remove_task(
        self,
        task_to_remove: asyncio.Task[Any],
        background_tasks: Set[asyncio.Task],
    ) -> None:
        if task_to_remove in background_tasks:
            background_tasks.remove(task_to_remove)
        try:
            exception = task_to_remove.exception()
        except asyncio.CancelledError:
            logging.debug("Task was cancelled", exc_info=False)
            return
        except Exception:
            logging.error("Error retrieving task exception", exc_info=True)
            return
        if exception:
            logging.error(
                "Task resulted in an exception",
                exc_info=exception,
            )

    def create_task(self, fn: Any, tg: asyncio.TaskGroup) -> None:
        task = tg.create_task(fn)
        self.background_tasks.add(task)
        task.add_done_callback(lambda x: self.remove_task(x, self.background_tasks))
