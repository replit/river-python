import asyncio
import logging
from typing import Any, Awaitable, Callable, Coroutine, TypeAlias

import nanoid
import websockets
from aiochannel import Channel
from opentelemetry.trace import Span, use_span
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from replit_river.common_session import (
    SendMessage,
    SessionState,
    TerminalStates,
)
from replit_river.message_buffer import MessageBuffer, MessageBufferClosedError
from replit_river.messages import (
    FailedSendingMessageException,
    WebsocketClosedException,
    send_transport_message,
)
from replit_river.seq_manager import (
    SeqManager,
)
from replit_river.task_manager import BackgroundTaskManager
from replit_river.transport_options import TransportOptions
from replit_river.websocket_wrapper import WebsocketWrapper, WsState

from .rpc import (
    ACK_BIT,
    TransportMessage,
    TransportMessageTracingSetter,
)

logger = logging.getLogger(__name__)

trace_propagator = TraceContextTextMapPropagator()
trace_setter = TransportMessageTracingSetter()

CloseSessionCallback: TypeAlias = Callable[["Session"], Coroutine[Any, Any, Any]]
RetryConnectionCallback: TypeAlias = Callable[
    [],
    Coroutine[Any, Any, Any],
]


class Session:
    """Common functionality shared between client_session and server_session"""

    _transport_id: str
    _to_id: str
    session_id: str
    _transport_options: TransportOptions

    # session state
    _state: SessionState
    _state_lock: asyncio.Lock
    _close_session_callback: CloseSessionCallback
    _close_session_after_time_secs: float | None

    # ws state
    _ws_lock: asyncio.Lock
    _ws_wrapper: WebsocketWrapper
    _heartbeat_misses: int
    _retry_connection_callback: RetryConnectionCallback | None

    # stream for tasks
    _streams: dict[str, Channel[Any]]

    # book keeping
    _seq_manager: SeqManager
    _buffer: MessageBuffer
    _task_manager: BackgroundTaskManager

    def __init__(
        self,
        transport_id: str,
        to_id: str,
        session_id: str,
        websocket: websockets.WebSocketCommonProtocol,
        transport_options: TransportOptions,
        close_session_callback: CloseSessionCallback,
        retry_connection_callback: RetryConnectionCallback | None = None,
    ) -> None:
        self._transport_id = transport_id
        self._to_id = to_id
        self.session_id = session_id
        self._transport_options = transport_options

        # session state
        self._state = SessionState.ACTIVE
        self._state_lock = asyncio.Lock()
        self._close_session_callback = close_session_callback
        self._close_session_after_time_secs: float | None = None

        # ws state
        self._ws_lock = asyncio.Lock()
        self._ws_wrapper = WebsocketWrapper(websocket)
        self._heartbeat_misses = 0
        self._retry_connection_callback = retry_connection_callback

        # stream for tasks
        self._streams: dict[str, Channel[Any]] = {}

        # book keeping
        self._seq_manager = SeqManager()
        self._buffer = MessageBuffer(self._transport_options.buffer_size)
        self._task_manager = BackgroundTaskManager()

    def _setup_heartbeats_task(
        self,
        do_close_websocket: Callable[[], Awaitable[None]],
    ) -> None:
        def increment_and_get_heartbeat_misses() -> int:
            self._heartbeat_misses += 1
            return self._heartbeat_misses

        self._task_manager.create_task(
            setup_heartbeat(
                self.session_id,
                self._transport_options.heartbeat_ms,
                self._transport_options.heartbeats_until_dead,
                lambda: (
                    self._state
                    if self._ws_wrapper.ws_state == WsState.OPEN
                    else SessionState.CONNECTING
                ),
                lambda: self._close_session_after_time_secs,
                close_websocket=do_close_websocket,
                send_message=self.send_message,
                increment_and_get_heartbeat_misses=increment_and_get_heartbeat_misses,
            )
        )
        self._task_manager.create_task(
            check_to_close_session(
                self._transport_id,
                self._transport_options.close_session_check_interval_ms,
                lambda: self._state,
                self._get_current_time,
                lambda: self._close_session_after_time_secs,
                self.close,
            )
        )

    async def is_session_open(self) -> bool:
        async with self._state_lock:
            return self._state == SessionState.ACTIVE

    async def is_websocket_open(self) -> bool:
        async with self._ws_lock:
            return self._ws_wrapper.is_open()

    async def _begin_close_session_countdown(self) -> None:
        """Begin the countdown to close session, this should be called when
        websocket is closed.
        """
        # calculate the value now before establishing it so that there are no
        # await points between the check and the assignment to avoid a TOCTOU
        # race.
        grace_period_ms = self._transport_options.session_disconnect_grace_ms
        close_session_after_time_secs = (
            await self._get_current_time() + grace_period_ms / 1000
        )
        if self._close_session_after_time_secs is not None:
            # already in grace period, no need to set again
            return
        logger.info(
            "websocket closed from %s to %s begin grace period",
            self._transport_id,
            self._to_id,
        )
        self._close_session_after_time_secs = close_session_after_time_secs

    async def replace_with_new_websocket(
        self, new_ws: websockets.WebSocketCommonProtocol
    ) -> None:
        async with self._ws_lock:
            old_wrapper = self._ws_wrapper
            old_ws_id = old_wrapper.ws.id
            if new_ws.id != old_ws_id:
                await old_wrapper.close()
            self._ws_wrapper = WebsocketWrapper(new_ws)

        # Send buffered messages to the new ws
        buffered_messages = list(self._buffer.buffer)
        for msg in buffered_messages:
            try:
                await send_transport_message(
                    msg,
                    new_ws,
                    self._begin_close_session_countdown,
                )
            except WebsocketClosedException:
                logger.info(
                    "Connection closed while sending buffered messages", exc_info=True
                )
                break
            except FailedSendingMessageException:
                logger.exception("Error while sending buffered messages")
                break

    async def _get_current_time(self) -> float:
        return asyncio.get_event_loop().time()

    def _reset_session_close_countdown(self) -> None:
        self._heartbeat_misses = 0
        self._close_session_after_time_secs = None

    async def get_next_expected_seq(self) -> int:
        """Get the next expected sequence number from the server."""
        return self._seq_manager.get_ack()

    async def get_next_sent_seq(self) -> int:
        """Get the next sequence number that the client will send."""
        return self._buffer.get_next_sent_seq() or self._seq_manager.get_seq()

    async def get_next_expected_ack(self) -> int:
        """Get the next expected ack that the client expects."""
        return self._seq_manager.get_seq()

    async def send_message(
        self,
        stream_id: str,
        payload: dict[Any, Any] | str,
        control_flags: int = 0,
        service_name: str | None = None,
        procedure_name: str | None = None,
        span: Span | None = None,
    ) -> None:
        """Send serialized messages to the websockets."""
        # if the session is not active, we should not do anything
        if self._state != SessionState.ACTIVE:
            return
        await self._buffer.has_capacity()
        # Start of critical section. No await between here and buffer.put()!
        msg = TransportMessage(
            streamId=stream_id,
            id=nanoid.generate(),
            from_=self._transport_id,
            to=self._to_id,
            seq=self._seq_manager.get_seq_and_increment(),
            ack=self._seq_manager.get_ack(),
            controlFlags=control_flags,
            payload=payload,
            serviceName=service_name,
            procedureName=procedure_name,
        )
        if span:
            with use_span(span):
                trace_propagator.inject(msg, None, trace_setter)
        try:
            try:
                self._buffer.put(msg)
            except MessageBufferClosedError:
                # The session is closed and is no longer accepting new messages.
                return
            async with self._ws_lock:
                if not self._ws_wrapper.is_open():
                    # If the websocket is closed, we should not send the message
                    # and wait for the retry from the buffer.
                    return
            await send_transport_message(
                msg, self._ws_wrapper.ws, self._begin_close_session_countdown
            )
        except WebsocketClosedException as e:
            logger.debug(
                "Connection closed while sending message %r, waiting for "
                "retry from buffer",
                type(e),
                exc_info=e,
            )
        except FailedSendingMessageException:
            logger.error(
                "Failed sending message, waiting for retry from buffer", exc_info=True
            )

    async def close_websocket(
        self, ws_wrapper: WebsocketWrapper, should_retry: bool
    ) -> None:
        """Mark the websocket as closed, close the websocket, and retry if needed."""
        async with self._ws_lock:
            # Already closed.
            if not ws_wrapper.is_open():
                return
            await ws_wrapper.close()
        if should_retry and self._retry_connection_callback:
            self._task_manager.create_task(self._retry_connection_callback())

    async def close(self) -> None:
        """Close the session and all associated streams."""
        logger.info(
            f"{self._transport_id} closing session "
            f"to {self._to_id}, ws: {self._ws_wrapper.id}, "
            f"current_state : {self._ws_wrapper.ws_state.name}"
        )
        async with self._state_lock:
            if self._state != SessionState.ACTIVE:
                # already closing
                return
            self._state = SessionState.CLOSING
            self._reset_session_close_countdown()
            await self._task_manager.cancel_all_tasks()

            await self.close_websocket(self._ws_wrapper, should_retry=False)

            await self._buffer.close()

            # Clear the session in transports
            await self._close_session_callback(self)

            # TODO: unexpected_close should close stream differently here to
            # throw exception correctly.
            for stream in self._streams.values():
                stream.close()
            self._streams.clear()

            self._state = SessionState.CLOSED


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
        if get_state() in TerminalStates:
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


async def setup_heartbeat(
    session_id: str,
    heartbeat_ms: float,
    heartbeats_until_dead: int,
    get_state: Callable[[], SessionState],
    get_closing_grace_period: Callable[[], float | None],
    close_websocket: Callable[[], Awaitable[None]],
    send_message: SendMessage[None],
    increment_and_get_heartbeat_misses: Callable[[], int],
) -> None:
    while True:
        await asyncio.sleep(heartbeat_ms / 1000)
        state = get_state()
        if state != SessionState.ACTIVE:
            logger.debug("Websocket is not connected, not sending heartbeat")
            continue
        if state in TerminalStates:
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
