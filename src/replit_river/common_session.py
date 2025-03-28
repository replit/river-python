import asyncio
import enum
import logging
from typing import Any, Awaitable, Callable, Protocol

from opentelemetry.trace import Span

logger = logging.getLogger(__name__)


class SendMessage(Protocol):
    async def __call__(
        self,
        *,
        stream_id: str,
        payload: dict[Any, Any] | str,
        control_flags: int,
        service_name: str | None,
        procedure_name: str | None,
        span: Span | None,
    ) -> None: ...


class SessionState(enum.Enum):
    """The state a session can be in.

    Can only transition from ACTIVE to CLOSING to CLOSED.
    """

    ACTIVE = 0
    CLOSING = 1
    CLOSED = 2


async def check_to_close_session(
    transport_id: str,
    close_session_check_interval_ms: float,
    get_state: Callable[[], SessionState],
    get_current_time: Callable[[], Awaitable[float]],
    get_close_session_after_time_secs: Callable[[], float | None],
    do_close: Callable[[], Awaitable[None]],
) -> None:
    while True:
        await asyncio.sleep(close_session_check_interval_ms / 1000)
        if get_state() != SessionState.ACTIVE:
            # already closing
            return
        # calculate the value now before comparing it so that there are no
        # await points between the check and the comparison to avoid a TOCTOU
        # race.
        current_time = await get_current_time()
        close_session_after_time_secs = get_close_session_after_time_secs()
        if not close_session_after_time_secs:
            continue
        if current_time > close_session_after_time_secs:
            logger.info("Grace period ended for %s, closing session", transport_id)
            await do_close()
            return
