import asyncio
import logging
from typing import Any, Optional, Set

from aiochannel import ChannelClosed
from replit_river.error_schema import ERROR_CODE_STREAM_CLOSED, RiverException


class BackgroundTaskManager:
    """Manages background tasks and logs exceptions."""

    def __init__(self) -> None:
        self.background_tasks: Set[asyncio.Task] = set()

    async def cancel_all_tasks(self) -> None:
        # Convert it to a list to avoid RuntimeError: Set changed size during iteration
        for task in list(self.background_tasks):
            await self.cancel_task(task, self.background_tasks)

    async def cancel_task(
        self,
        task_to_remove: asyncio.Task[Any],
        background_tasks: Set[asyncio.Task],
    ) -> None:
        task_to_remove.cancel()
        try:
            await task_to_remove
            if task_to_remove in background_tasks:
                background_tasks.remove(task_to_remove)
            exception = task_to_remove.exception()
        except (asyncio.CancelledError, ChannelClosed):
            logging.debug("Task was cancelled %r", task_to_remove)
            return
        except Exception:
            logging.error("Error retrieving task exception", exc_info=True)
            return
        if exception:
            if (
                isinstance(exception, RiverException)
                and exception.code == ERROR_CODE_STREAM_CLOSED
            ):
                # Task is cancelled
                pass
            else:
                logging.error(
                    "Task resulted in an exception",
                    exc_info=exception,
                )

    def _task_done_callback(
        self,
        task_to_remove: asyncio.Task[Any],
        background_tasks: Set[asyncio.Task],
    ) -> None:
        if task_to_remove in background_tasks:
            background_tasks.remove(task_to_remove)
        try:
            exception = task_to_remove.exception()
        except asyncio.CancelledError:
            return
        except Exception:
            logging.error("Error retrieving task exception", exc_info=True)
            return
        if exception:
            if (
                isinstance(exception, RiverException)
                and exception.code == ERROR_CODE_STREAM_CLOSED
            ):
                # Task is cancelled
                pass
            else:
                logging.error(
                    "Task resulted in an exception",
                    exc_info=exception,
                )

    async def create_task(
        self, fn: Any, tg: Optional[asyncio.TaskGroup] = None
    ) -> asyncio.Task:
        if tg:
            task = tg.create_task(fn)
        else:
            task = asyncio.create_task(fn)
        self.background_tasks.add(task)
        task.add_done_callback(
            lambda x: self._task_done_callback(x, self.background_tasks)
        )
        return task
