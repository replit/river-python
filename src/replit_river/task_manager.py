import asyncio
import logging
from typing import Coroutine, Set

from replit_river.error_schema import ERROR_CODE_STREAM_CLOSED, RiverException

logger = logging.getLogger(__name__)


class BackgroundTaskManager:
    """Manages background tasks and logs exceptions."""

    def __init__(self) -> None:
        self.background_tasks: Set[asyncio.Task[None]] = set()

    async def cancel_all_tasks(self) -> None:
        """Asynchronously cancels all tasks managed by this instance."""
        # Convert it to a list to avoid RuntimeError: Set changed size during iteration
        for task in list(self.background_tasks):
            await self.cancel_task(task, self.background_tasks)

    @staticmethod
    async def cancel_task(
        task_to_remove: asyncio.Task[None],
        background_tasks: Set[asyncio.Task[None]],
    ) -> None:
        """Cancels a given task and ensures it is removed from the set of managed tasks.

        Args:
            task_to_remove: The asyncio.Task instance to cancel.
            background_tasks: Set of all tasks being tracked.
        """
        task_to_remove.cancel()
        try:
            await task_to_remove
        except asyncio.CancelledError:
            # If we cancel the task manager we will get called here as well,
            # if we want to handle the cancellation differently we can do it here.
            logger.debug("Task was cancelled %r", task_to_remove)
        except RiverException as e:
            if e.code == ERROR_CODE_STREAM_CLOSED:
                # Task is cancelled
                pass
            logger.exception("Exception on cancelling task")
        except Exception:
            logger.exception("Exception on cancelling task")
        finally:
            # Remove the task from the set regardless of the outcome
            background_tasks.discard(task_to_remove)

    def _task_done_callback(
        self,
        task_to_remove: asyncio.Task[None],
        background_tasks: Set[asyncio.Task[None]],
    ) -> None:
        """Callback to be executed when a task is done. It removes the task from the set
          and logs any exceptions.

        Args:
            task_to_remove: The asyncio.Task that has completed.
            background_tasks: Set of all tasks being tracked.
        """
        if task_to_remove in background_tasks:
            background_tasks.remove(task_to_remove)
        try:
            exception = task_to_remove.exception()
        except asyncio.CancelledError:
            return
        except Exception:
            logger.error("Error retrieving task exception", exc_info=True)
            return
        if exception:
            if (
                isinstance(exception, RiverException)
                and exception.code == ERROR_CODE_STREAM_CLOSED
            ):
                # Task is cancelled
                pass
            else:
                logger.error(
                    "Exception on cancelling task",
                    exc_info=exception,
                )

    def create_task(
        self, fn: Coroutine[None, None, None], tg: asyncio.TaskGroup | None = None
    ) -> asyncio.Task[None]:
        """Creates a task from a callable and adds it to the background tasks set.

        Args:
            fn: A callable to be executed in the task.
            tg: Optional asyncio.TaskGroup for managing the task lifecycle.
            TODO: tg is hard to understand when passed all the way here, we should
            refactor to make this easier to understand.

        Returns:
            The created asyncio.Task.
        """
        if tg:
            task = tg.create_task(fn)
        else:
            task = asyncio.create_task(fn)
        self.background_tasks.add(task)
        task.add_done_callback(
            lambda x: self._task_done_callback(x, self.background_tasks)
        )
        return task
