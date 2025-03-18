import asyncio
import logging
from typing import Awaitable, Callable

from replit_river.messages import FailedSendingMessageException
from replit_river.rpc import ACK_BIT
from replit_river.session import SendMessage, SessionState


logger = logging.getLogger(__name__)

async def setup_heartbeat(
    session_id: str,
    heartbeat_ms: float,
    heartbeats_until_dead: int,
    get_state: Callable[[], SessionState],
    get_closing_grace_period: Callable[[], float | None],
    close_websocket: Callable[[], Awaitable[None]],
    send_message: SendMessage,
    increment_and_get_heartbeat_misses: Callable[[], int],
) -> None:
    logger.debug("Start heartbeat")
    while True:
        await asyncio.sleep(heartbeat_ms / 1000)
        state = get_state()
        if state != SessionState.ACTIVE:
            logger.debug(
                "Session is closed, no need to send heartbeat, state : "
                "%r close_session_after_this: %r",
                {state},
                {get_closing_grace_period()},
            )
            # session is closing / closed, no need to send heartbeat anymore
            return
        try:
            await send_message(
                stream_id="heartbeat",
                # TODO: make this a message class
                # https://github.com/replit/river/blob/741b1ea6d7600937ad53564e9cf8cd27a92ec36a/transport/message.ts#L42
                payload={
                    "ack": 0,
                },
                control_flags=ACK_BIT,
                procedure_name=None,
                service_name=None,
                span=None,
            )

            if increment_and_get_heartbeat_misses() > heartbeats_until_dead:
                if get_closing_grace_period() is not None:
                    # already in grace period, no need to set again
                    continue
                logger.info(
                    "%r closing websocket because of heartbeat misses",
                    session_id,
                )
                await close_websocket()
                continue
        except FailedSendingMessageException:
            # this is expected during websocket closed period
            continue
