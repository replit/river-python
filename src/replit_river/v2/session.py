import asyncio
import logging
from collections import deque
from typing import Any, Awaitable, Callable, Coroutine, TypeAlias

import nanoid  # type: ignore
from aiochannel import Channel
from opentelemetry.trace import Span, use_span
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from websockets.asyncio.client import ClientConnection
from websockets.frames import CloseCode

from replit_river.common_session import (
    SessionState,
    check_to_close_session,
    setup_heartbeat,
)
from replit_river.rpc import (
    TransportMessage,
    TransportMessageTracingSetter,
)
from replit_river.task_manager import BackgroundTaskManager
from replit_river.transport_options import TransportOptions

logger = logging.getLogger(__name__)

trace_propagator = TraceContextTextMapPropagator()
trace_setter = TransportMessageTracingSetter()

CloseSessionCallback: TypeAlias = Callable[["Session"], Coroutine[Any, Any, Any]]
RetryConnectionCallback: TypeAlias = Callable[
    [],
    Coroutine[Any, Any, Any],
]


class Session:
    _transport_id: str
    _to_id: str
    session_id: str
    _transport_options: TransportOptions

    # session state, only modified during closing
    _state: SessionState
    _close_session_callback: CloseSessionCallback
    _close_session_after_time_secs: float | None

    # ws state
    _ws_connected: bool
    _ws_unwrapped: ClientConnection | None
    _heartbeat_misses: int
    _retry_connection_callback: RetryConnectionCallback | None

    # stream for tasks
    _streams: dict[str, Channel[Any]]

    # book keeping
    _ack_buffer: deque[TransportMessage]
    _send_buffer: deque[TransportMessage]
    _task_manager: BackgroundTaskManager
    ack: int  # Most recently acknowledged seq
    seq: int  # Last sent sequence number

    def __init__(
        self,
        transport_id: str,
        to_id: str,
        session_id: str,
        websocket: ClientConnection,
        transport_options: TransportOptions,
        close_session_callback: CloseSessionCallback,
        retry_connection_callback: RetryConnectionCallback | None = None,
    ) -> None:
        self._transport_id = transport_id
        self._to_id = to_id
        self.session_id = session_id
        self._transport_options = transport_options

        # session state, only modified during closing
        self._state = SessionState.ACTIVE
        self._close_session_callback = close_session_callback
        self._close_session_after_time_secs: float | None = None

        # ws state
        self._ws_connected = True
        self._ws_unwrapped = websocket
        self._heartbeat_misses = 0
        self._retry_connection_callback = retry_connection_callback

        # message state
        self._space_available_cond = asyncio.Condition()
        self._queue_full_lock = asyncio.Lock()

        # stream for tasks
        self._streams: dict[str, Channel[Any]] = {}

        # book keeping
        self._ack_buffer = deque()
        self._send_buffer = deque()
        self._task_manager = BackgroundTaskManager()
        self.ack = 0
        self.seq = 0

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
                lambda: self._state,
                lambda: self._ws_connected,
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

    def is_session_open(self) -> bool:
        return self._state == SessionState.ACTIVE

    def is_websocket_open(self) -> bool:
        return self._ws_connected

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
        self._ws_connected = False

    async def replace_with_new_websocket(self, new_ws: ClientConnection) -> None:
        if self._ws_unwrapped and new_ws.id != self._ws_unwrapped.id:
            self._task_manager.create_task(
                self._ws_unwrapped.close(
                    CloseCode.PROTOCOL_ERROR, "Transparent reconnect"
                )
            )
        self._ws_unwrapped = new_ws
        self._ws_connected = True

    async def _get_current_time(self) -> float:
        return asyncio.get_event_loop().time()

    def _reset_session_close_countdown(self) -> None:
        self._heartbeat_misses = 0
        self._close_session_after_time_secs = None

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
        msg = TransportMessage(
            streamId=stream_id,
            id=nanoid.generate(),
            from_=self._transport_id,
            to=self._to_id,
            seq=self.seq,
            ack=self.ack,
            controlFlags=control_flags,
            payload=payload,
            serviceName=service_name,
            procedureName=procedure_name,
        )

        if span:
            with use_span(span):
                trace_propagator.inject(msg, None, trace_setter)

        # As we prepare to push onto the buffer, if the buffer is full, we lock.
        # This lock will be released by the buffered_message_sender task, so it's
        # important that we don't release it here.
        #
        # The reason for this is that in Python, asyncio.Lock is "fair", first
        # come, first served.
        #
        # If somebody else is already waiting or we've filled the buffer, we
        # should get in line.
        if (
            self._queue_full_lock.locked()
            or len(self._send_buffer) >= self._transport_options.buffer_size
        ):
            logger.warning("LOCK ACQUIRED %r", repr(payload))
            await self._queue_full_lock.acquire()
            logger.warning("LOCK RELEASED %r", repr(payload))
        self._send_buffer.append(msg)
        self.seq += 1

    async def close(self) -> None:
        """Close the session and all associated streams."""
        logger.info(
            f"{self._transport_id} closing session "
            f"to {self._to_id}, ws: {self._ws_unwrapped}"
        )
        if self._state != SessionState.ACTIVE:
            # already closing
            return
        self._state = SessionState.CLOSING
        self._reset_session_close_countdown()
        await self._task_manager.cancel_all_tasks()

        if self._ws_unwrapped:
            # The Session isn't guaranteed to live much longer than this close()
            # invocation, so let's await this close to avoid dropping the socket.
            await self._ws_unwrapped.close()

        # Clear the session in transports
        await self._close_session_callback(self)

        # TODO: unexpected_close should close stream differently here to
        # throw exception correctly.
        for stream in self._streams.values():
            stream.close()
        self._streams.clear()

        self._state = SessionState.CLOSED
