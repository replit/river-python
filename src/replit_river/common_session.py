import asyncio
import enum
import logging
from typing import Any, Awaitable, Callable, Coroutine, Protocol

from opentelemetry.trace import Span
from websockets import WebSocketCommonProtocol
from websockets.asyncio.client import ClientConnection

from replit_river.messages import (
    FailedSendingMessageException,
    WebsocketClosedException,
    send_transport_message,
)
from replit_river.rpc import TransportMessage

logger = logging.getLogger(__name__)


class SendMessage[Result](Protocol):
    async def __call__(
        self,
        *,
        stream_id: str,
        payload: dict[Any, Any] | str,
        control_flags: int,
        service_name: str | None,
        procedure_name: str | None,
        span: Span | None,
    ) -> Result: ...


class SessionState(enum.Enum):
    """The state a session can be in.

    Valid transitions:
    - NO_CONNECTION -> {CONNECTING, CLOSING}
    - CONNECTING -> {NO_CONNECTION, ACTIVE, CLOSING}
    - ACTIVE -> {NO_CONNECTION, CONNECTING, CLOSING}
    - CLOSING -> {CLOSED}
    - CLOSED -> {}
    """

    NO_CONNECTION = 0
    CONNECTING = 1
    ACTIVE = 2
    CLOSING = 3
    CLOSED = 4


ConnectingStates = set([SessionState.NO_CONNECTION, SessionState.CONNECTING])
TerminalStates = set([SessionState.CLOSING, SessionState.CLOSED])


async def buffered_message_sender(
    block_until_connected: Callable[[], Awaitable[None]],
    block_until_message_available: Callable[[], Awaitable[None]],
    get_ws: Callable[[], WebSocketCommonProtocol | ClientConnection | None],
    websocket_closed_callback: Callable[[], Coroutine[Any, Any, None]],
    get_next_pending: Callable[[], TransportMessage | None],
    commit: Callable[[TransportMessage], None],
    get_state: Callable[[], SessionState],
) -> None:
    our_task = asyncio.current_task()
    while our_task and not our_task.cancelling() and not our_task.cancelled():
        await block_until_message_available()

        if get_state() in TerminalStates:
            logger.debug("_buffered_message_sender: closing")
            return

        while (ws := get_ws()) is None:
            # Block until we have a handle
            logger.debug(
                "_buffered_message_sender: Waiting until ws is connected",
            )
            await block_until_connected()

        if not ws:
            logger.debug("_buffered_message_sender: ws is not connected, loop")
            continue

        if msg := get_next_pending():
            logger.debug(
                "_buffered_message_sender: Dequeued %r to send over %r",
                msg,
                ws,
            )
            try:
                await send_transport_message(msg, ws, websocket_closed_callback)
                commit(msg)
            except WebsocketClosedException as e:
                logger.debug(
                    "_buffered_message_sender: Connection closed while sending "
                    "message %r, waiting for retry from buffer",
                    type(e),
                    exc_info=e,
                )
                break
            except FailedSendingMessageException:
                logger.error(
                    "Failed sending message, waiting for retry from buffer",
                    exc_info=True,
                )
                break
            except Exception:
                logger.exception("Error attempting to send buffered messages")
                break
