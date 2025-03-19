import asyncio
import enum
import logging
from typing import Any, Awaitable, Callable, Protocol

from aiochannel import Channel, ChannelClosed
from opentelemetry.trace import Span

from replit_river.messages import FailedSendingMessageException
from replit_river.rpc import ACK_BIT, STREAM_CLOSED_BIT, TransportMessage
from replit_river.seq_manager import InvalidMessageException

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


async def add_msg_to_stream(
    msg: TransportMessage,
    stream: Channel,
) -> None:
    if (
        msg.controlFlags & STREAM_CLOSED_BIT != 0
        and msg.payload.get("type", None) == "CLOSE"
    ):
        # close message is not sent to the stream
        return
    try:
        await stream.put(msg.payload)
    except ChannelClosed:
        # The client is no longer interested in this stream,
        # just drop the message.
        pass
    except RuntimeError as e:
        raise InvalidMessageException(e) from e
