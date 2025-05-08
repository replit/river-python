import asyncio
import logging
from collections import deque
from collections.abc import AsyncIterable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta
from typing import (
    Any,
    AsyncGenerator,
    AsyncIterator,
    Awaitable,
    Callable,
    Coroutine,
    Literal,
    NotRequired,
    TypeAlias,
    TypedDict,
    assert_never,
)

import nanoid
import websockets.asyncio.client
from aiochannel import Channel, ChannelEmpty, ChannelFull
from aiochannel.errors import ChannelClosed
from opentelemetry.trace import Span, use_span
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from pydantic import ValidationError
from websockets import ConnectionClosedOK
from websockets.asyncio.client import ClientConnection
from websockets.exceptions import ConnectionClosed

from replit_river.common_session import (
    ActiveStates,
    ConnectingStates,
    SendMessage,
    SessionState,
    TerminalStates,
    buffered_message_sender,
)
from replit_river.error_schema import (
    ERROR_CODE_CANCEL,
    ERROR_CODE_SESSION_STATE_MISMATCH,
    ERROR_CODE_STREAM_CLOSED,
    ERROR_HANDSHAKE,
    RiverError,
    RiverException,
    RiverServiceException,
    SessionClosedRiverServiceException,
    exception_from_message,
)
from replit_river.messages import (
    FailedSendingMessageException,
    WebsocketClosedException,
    parse_transport_msg,
    send_transport_message,
)
from replit_river.rate_limiter import RateLimiter
from replit_river.rpc import (
    ACK_BIT,
    STREAM_OPEN_BIT,
    ControlMessageHandshakeRequest,
    ControlMessageHandshakeResponse,
    ExpectedSessionState,
    TransportMessage,
    TransportMessageTracingSetter,
)
from replit_river.seq_manager import (
    InvalidMessageException,
    OutOfOrderMessageException,
)
from replit_river.task_manager import BackgroundTaskManager
from replit_river.transport_options import (
    MAX_MESSAGE_BUFFER_SIZE,
    TransportOptions,
    UriAndMetadata,
)

PROTOCOL_VERSION = "v2.0"

STREAM_CANCEL_BIT_TYPE = Literal[0b00100]
STREAM_CANCEL_BIT: STREAM_CANCEL_BIT_TYPE = 0b00100
STREAM_CLOSED_BIT_TYPE = Literal[0b01000]
STREAM_CLOSED_BIT: STREAM_CLOSED_BIT_TYPE = 0b01000


SESSION_CLOSE_TIMEOUT_SEC = 2

_BackpressuredWaiter: TypeAlias = Callable[[], Awaitable[None]]


class ResultOk(TypedDict):
    ok: Literal[True]
    payload: Any


class ErrorPayload(TypedDict):
    code: str
    message: str


class ResultError(TypedDict):
    # Account for structurally incoherent payloads
    ok: NotRequired[Literal[False]]
    payload: ErrorPayload


ResultType: TypeAlias = ResultOk | ResultError

logger = logging.getLogger(__name__)

trace_propagator = TraceContextTextMapPropagator()
trace_setter = TransportMessageTracingSetter()

CloseSessionCallback: TypeAlias = Callable[["Session"], None]
RetryConnectionCallback: TypeAlias = Callable[
    [],
    Coroutine[Any, Any, Any],
]


@dataclass
class _IgnoreMessage:
    pass


class StreamMeta(TypedDict):
    span: Span
    release_backpressured_waiter: Callable[[], None]
    error_channel: Channel[Exception]
    output: Channel[ResultType]


class Session[HandshakeMetadata]:
    _server_id: str
    session_id: str
    _transport_options: TransportOptions

    # session state, only modified during closing
    _state: SessionState
    _close_session_callback: CloseSessionCallback
    _close_session_after_time_secs: float | None
    _connecting_task: asyncio.Task[None] | None
    _wait_for_connected: asyncio.Event

    _client_id: str
    _rate_limiter: RateLimiter
    _uri_and_metadata_factory: Callable[
        [], Awaitable[UriAndMetadata[HandshakeMetadata]]
    ]

    # ws state
    _ws: ClientConnection | None
    _retry_connection_callback: RetryConnectionCallback | None

    # message state
    _process_messages: asyncio.Event
    _space_available: asyncio.Event

    # stream for tasks
    _streams: dict[str, StreamMeta]

    # book keeping
    _ack_buffer: deque[TransportMessage]
    _send_buffer: deque[TransportMessage]
    _task_manager: BackgroundTaskManager
    ack: int  # Most recently acknowledged seq
    seq: int  # Last sent sequence number

    # Terminating
    _terminating_task: asyncio.Task[None] | None

    def __init__(
        self,
        server_id: str,
        session_id: str,
        transport_options: TransportOptions,
        close_session_callback: CloseSessionCallback,
        client_id: str,
        rate_limiter: RateLimiter,
        uri_and_metadata_factory: Callable[
            [], Awaitable[UriAndMetadata[HandshakeMetadata]]
        ],
        retry_connection_callback: RetryConnectionCallback | None = None,
    ) -> None:
        self._server_id = server_id
        self.session_id = session_id
        self._transport_options = transport_options

        # session state
        self._state = SessionState.NO_CONNECTION
        self._close_session_callback = close_session_callback
        self._close_session_after_time_secs: float | None = None
        self._connecting_task = None
        self._wait_for_connected = asyncio.Event()

        self._client_id = client_id
        # TODO: LeakyBucketRateLimit accepts "user" for all methods, which has
        # historically been and continues to be "client_id".
        #
        # There's 1:1 client <-> transport, which means LeakyBucketRateLimit is only
        # tracking exactly one rate limit.
        #
        # The "user" parameter is YAGNI, dethread client_id after v1 is deleted.
        self._rate_limiter = rate_limiter
        self._uri_and_metadata_factory = uri_and_metadata_factory

        # ws state
        self._ws = None
        self._retry_connection_callback = retry_connection_callback

        # message state
        self._process_messages = asyncio.Event()
        self._space_available = asyncio.Event()
        # Ensure we initialize the above Event to "set" to avoid being blocked from
        # the beginning.
        self._space_available.set()

        # stream for tasks
        self._streams: dict[str, StreamMeta] = {}

        # book keeping
        self._ack_buffer = deque()
        self._send_buffer = deque()
        self._task_manager = BackgroundTaskManager()
        self.ack = 0
        self.seq = 0

        # Terminating
        self._terminating_task = None

        self._start_recv_from_ws()
        self._start_buffered_message_sender()

    async def ensure_connected(self) -> None:
        """
        Either return immediately or establish a websocket connection and return
        once we can accept messages.

        One of the goals of this function is to gate exactly one call to the
        logic that actually establishes the connection.
        """

        logger.debug("ensure_connected: is_connected=%r", self.is_connected())
        if self.is_connected():
            return

        def get_next_sent_seq() -> int:
            if self._send_buffer:
                return self._send_buffer[0].seq
            return self.seq

        def transition_connecting(ws: ClientConnection) -> None:
            if self._state in TerminalStates:
                return
            logger.debug("transition_connecting")
            self._state = SessionState.CONNECTING
            # "Clear" here means observers should wait until we are connected.
            self._wait_for_connected.clear()

            # Expose the current ws to be collected by close()
            self._ws = ws

        def transition_connected(ws: ClientConnection) -> None:
            if self._state in TerminalStates:
                return
            logger.debug("transition_connected")
            self._state = SessionState.ACTIVE
            self._ws = ws

            # We're connected, wake everybody up using set()
            self._wait_for_connected.set()

        def close_ws_in_background(ws: ClientConnection) -> None:
            self._task_manager.create_task(ws.close())

        def unbind_connecting_task() -> None:
            # We are in a state where we may throw an exception.
            #
            # To allow subsequent calls to ensure_connected to pass, we clear ourselves.
            # This is safe because each individual function that is waiting on this
            # function completeing already has a reference, so we'll last a few ticks
            # before GC.
            current_task = asyncio.current_task()
            if self._connecting_task is current_task:
                self._connecting_task = None
            else:
                logger.debug("unbind_connecting_task failed, id did not match")

        if not self._connecting_task or self._connecting_task.done():
            self._connecting_task = asyncio.create_task(
                _do_ensure_connected(
                    transport_options=self._transport_options,
                    client_id=self._client_id,
                    server_id=self._server_id,
                    session_id=self.session_id,
                    rate_limiter=self._rate_limiter,
                    uri_and_metadata_factory=self._uri_and_metadata_factory,
                    get_next_sent_seq=get_next_sent_seq,
                    get_current_ack=lambda: self.ack,
                    get_current_time=self._get_current_time,
                    get_state=lambda: self._state,
                    transition_connecting=transition_connecting,
                    close_ws_in_background=close_ws_in_background,
                    transition_connected=transition_connected,
                    unbind_connecting_task=unbind_connecting_task,
                    close_session=self._close_internal_nowait,
                )
            )

        try:
            await self._connecting_task
        except asyncio.CancelledError:
            pass

        if self._terminating_task:
            try:
                await self._terminating_task
            except asyncio.CancelledError:
                pass

    def is_terminal(self) -> bool:
        """
        If the session is in a terminal state.
        Do not send messages, do not expect any more messages to be emitted,
        the state is expected to be stale.
        """
        return self._state in TerminalStates

    def is_connected(self) -> bool:
        return self._state in ActiveStates

    async def _get_current_time(self) -> float:
        return asyncio.get_event_loop().time()

    async def _enqueue_message(
        self,
        stream_id: str,
        payload: dict[Any, Any] | str,
        control_flags: int = 0,
        service_name: str | None = None,
        procedure_name: str | None = None,
        span: Span | None = None,
    ) -> None:
        """Send serialized messages to the websockets."""
        logger.debug(
            "_enqueue_message(stream_id=%r, payload=%r, control_flags=%r, "
            "service_name=%r, procedure_name=%r)",
            stream_id,
            payload,
            bin(control_flags),
            service_name,
            procedure_name,
        )
        # Ensure the buffer isn't full before we enqueue
        await self._space_available.wait()

        # Before we append, do an important check
        if self._state in TerminalStates:
            # session is closing / closed, raise
            raise SessionClosedRiverServiceException(
                "river session is closed, dropping message",
                stream_id,
            )

        # Begin critical section: Avoid any await between here and _send_buffer.append
        msg = TransportMessage(
            streamId=stream_id,
            id=nanoid.generate(),
            from_=self._client_id,
            to=self._server_id,
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

        # We're clear to add to the send buffer
        self._send_buffer.append(msg)

        # Increment immediately so we maintain consistency
        self.seq += 1

        # If the buffer is now full, reset the block
        if len(self._send_buffer) >= self._transport_options.buffer_size:
            self._space_available.clear()

        # Wake up buffered_message_sender
        self._process_messages.set()

    async def close(
        self,
        reason: Exception | None = None,
    ) -> None:
        """Close the session and all associated streams."""
        if self._terminating_task:
            try:
                logger.debug("Session already closing, waiting...")
                async with asyncio.timeout(SESSION_CLOSE_TIMEOUT_SEC):
                    await self._terminating_task
            except asyncio.TimeoutError:
                logger.warning(
                    f"Session took longer than {SESSION_CLOSE_TIMEOUT_SEC} "
                    "seconds to close, leaking",
                )
            return
        try:
            await self._close_internal(reason)
        except asyncio.CancelledError:
            pass

    def _close_internal_nowait(self, reason: Exception | None = None) -> None:
        """
        When calling close() from asyncio Tasks, we must not block.

        This function does so, deferring to the underlying infrastructure for
        creating self._terminating_task.
        """
        self._close_internal(reason)

    def _close_internal(self, reason: Exception | None = None) -> asyncio.Task[None]:
        """
        Internal close method. Subsequent calls past the first do not block.

        This is intended to be the primary driver of a session being torn down
        and returned to its initial state.

        NB: This function is intended to be the sole lifecycle manager of
            self._terminating_task. Waiting on the completion of that task is optional,
            but the population of that property is critical.

        NB: We must not await the task returned from this function from chained tasks
            inside this session, otherwise we will create a thread loop.
        """

        async def do_close() -> None:
            logger.info(
                f"{self.session_id} closing session to {self._server_id}, "
                f"ws: {self._ws}"
            )
            self._state = SessionState.CLOSING

            # We're closing, so we need to wake up...
            # ... tasks waiting for connection to be established
            self._wait_for_connected.set()
            # ... consumers waiting to enqueue messages
            self._space_available.set()
            # ... message processor so it can exit cleanly
            self._process_messages.set()

            # Wait to permit the waiting tasks to shut down gracefully
            await asyncio.sleep(0.25)

            await self._task_manager.cancel_all_tasks()

            for stream_id, stream_meta in self._streams.items():
                stream_meta["output"].close()
                # Wake up backpressured writers
                try:
                    stream_meta["error_channel"].put_nowait(
                        reason
                        or SessionClosedRiverServiceException(
                            "river session is closed",
                            stream_id,
                        )
                    )
                except ChannelFull:
                    logger.exception(
                        "Unable to tell the caller that the session is going away",
                    )
                stream_meta["release_backpressured_waiter"]()
            # Before we GC the streams, let's wait for all tasks to be closed gracefully
            try:
                async with asyncio.timeout(
                    self._transport_options.shutdown_all_streams_timeout_ms
                ):
                    # Block for backpressure and emission errors from the ws
                    await asyncio.gather(
                        *[
                            stream_meta["output"].join()
                            for stream_meta in self._streams.values()
                        ]
                    )
            except asyncio.TimeoutError:
                spans: list[Span] = [
                    stream_meta["span"]
                    for stream_meta in self._streams.values()
                    if not stream_meta["output"].closed()
                ]
                span_ids = [span.get_span_context().span_id for span in spans]
                logger.exception(
                    "Timeout waiting for output streams to finallize",
                    extra={"span_ids": span_ids},
                )
            self._streams.clear()

            if self._ws:
                # The Session isn't guaranteed to live much longer than this close()
                # invocation, so let's await this close to avoid dropping the socket.
                await self._ws.close()

            self._state = SessionState.CLOSED

            # Clear the session in transports
            # This will get us GC'd, so this should be the last thing.
            self._close_session_callback(self)

        if not self._terminating_task:
            self._terminating_task = asyncio.create_task(do_close())

        return self._terminating_task

    def _start_buffered_message_sender(
        self,
    ) -> None:
        """
        Building on buffered_message_sender's documentation, we implement backpressure
        per-stream by way of self._streams'

            error_channel: Channel[Exception]
            backpressured_waiter: Callable[[], Awaitable[None]]

        This is accomplished via the following strategy:
        - If buffered_message_sender encounters an error, we transition back to
          connecting and attempt to handshake.

          If the handshake fails, we close the session with an informative error that
          gets emitted to all backpressured client methods.

        -  Alternately, if buffered_message_sender successfully writes back to the

        - Finally, if _recv_from_ws encounters an error (transport or deserialization),
          it transitions to NO_CONNECTION and defers to the client_transport to
          reestablish a connection.

          The in-flight messages are still valid, as if we can reconnect to the server
          in time, those responses can be marshalled to their respective callbacks.
        """

        async def commit(msg: TransportMessage) -> None:
            pending = self._send_buffer.popleft()
            if msg.seq != pending.seq:
                logger.error("Out of sequence error")
            self._ack_buffer.append(pending)

            # On commit...
            # ... release pending writers waiting for more buffer space
            self._space_available.set()
            # ... tell the message sender to back off if there are no pending messages
            if not self._send_buffer:
                self._process_messages.clear()

            # Wake up backpressured writer
            stream_meta = self._streams.get(pending.streamId)
            if stream_meta:
                stream_meta["release_backpressured_waiter"]()

        def get_next_pending() -> TransportMessage | None:
            if self._send_buffer:
                return self._send_buffer[0]
            return None

        def get_ws() -> ClientConnection | None:
            if self.is_connected():
                return self._ws
            return None

        async def block_until_connected() -> None:
            if self._state in TerminalStates:
                return
            logger.debug("block_until_connected")
            await self._wait_for_connected.wait()
            logger.debug("block_until_connected released!")

        async def block_until_message_available() -> None:
            await self._process_messages.wait()

        self._task_manager.create_task(
            buffered_message_sender(
                block_until_connected=block_until_connected,
                block_until_message_available=block_until_message_available,
                get_ws=get_ws,
                websocket_closed_callback=self.ensure_connected,
                get_next_pending=get_next_pending,
                commit=commit,
                get_state=lambda: self._state,
            )
        )

    def _start_recv_from_ws(self) -> None:
        async def transition_no_connection() -> None:
            if self._state in TerminalStates:
                return
            self._state = SessionState.NO_CONNECTION
            self._wait_for_connected.clear()
            if self._ws:
                self._task_manager.create_task(self._ws.close())
                self._ws = None

            if self._retry_connection_callback:
                self._task_manager.create_task(self._retry_connection_callback())
            else:
                await self.ensure_connected()

        def assert_incoming_seq_bookkeeping(
            msg_from: str,
            msg_seq: int,
            msg_ack: int,
        ) -> Literal[True] | _IgnoreMessage:
            # Update bookkeeping
            if msg_seq < self.ack:
                logger.info(
                    "Received duplicate msg",
                    extra={
                        "from": msg_from,
                        "got_seq": msg_seq,
                        "expected_ack": self.ack,
                    },
                )
                return _IgnoreMessage()
            elif msg_seq > self.ack:
                logger.warning(
                    f"Out of order message received got {msg_seq} expected {self.ack}"
                )

                raise OutOfOrderMessageException(
                    received_seq=msg_seq,
                    expected_ack=self.ack,
                )
            else:
                # Set our next expected ack number
                self.ack = msg_seq + 1

                # Discard old server-ack'd messages from the ack buffer
                while self._ack_buffer and self._ack_buffer[0].seq < msg_ack:
                    self._ack_buffer.popleft()

            return True

        async def block_until_connected() -> None:
            if self._state in TerminalStates:
                return
            logger.debug("block_until_connected")
            await self._wait_for_connected.wait()
            logger.debug("block_until_connected released!")

        self._task_manager.create_task(
            _recv_from_ws(
                block_until_connected=block_until_connected,
                client_id=self._client_id,
                get_state=lambda: self._state,
                get_ws=lambda: self._ws,
                transition_no_connection=transition_no_connection,
                close_session=self._close_internal_nowait,
                assert_incoming_seq_bookkeeping=assert_incoming_seq_bookkeeping,
                get_stream=lambda stream_id: self._streams.get(stream_id),
                enqueue_message=self._enqueue_message,
            )
        )

    @asynccontextmanager
    async def _with_stream(
        self,
        span: Span,
        stream_id: str,
        maxsize: int,
    ) -> AsyncIterator[tuple[_BackpressuredWaiter, AsyncIterator[ResultType]]]:
        """
        _with_stream

        An async context that exposes a managed stream and an event that permits
        producers to respond to backpressure.

        It is expected that the first message emitted ignores this error_channel,
        since the first event does not care about backpressure, but subsequent events
        emitted should call await error_channel.wait() prior to emission.
        """
        output: Channel[ResultType] = Channel(maxsize=maxsize)
        backpressured_waiter_event: asyncio.Event = asyncio.Event()
        error_channel: Channel[Exception] = Channel(maxsize=1)
        self._streams[stream_id] = {
            "span": span,
            "error_channel": error_channel,
            "release_backpressured_waiter": backpressured_waiter_event.set,
            "output": output,
        }

        async def backpressured_waiter() -> None:
            await backpressured_waiter_event.wait()
            try:
                err = error_channel.get_nowait()
                raise err
            except (ChannelClosed, ChannelEmpty):
                # No errors, off to the next message
                pass

        async def error_checking_output() -> AsyncIterator[ResultType]:
            async for elem in output:
                try:
                    err = error_channel.get_nowait()
                    raise err
                except (ChannelClosed, ChannelEmpty):
                    # No errors, off to the next message
                    pass
                yield elem

        try:
            yield (backpressured_waiter, error_checking_output())
        finally:
            stream_meta = self._streams.get(stream_id)
            if not stream_meta:
                logger.warning(
                    "_with_stream had an entry deleted out from under it",
                    extra={
                        "session_id": self.session_id,
                        "stream_id": stream_id,
                    },
                )
                return
            # We need to signal back to all emitters or waiters that we're gone
            output.close()
            del self._streams[stream_id]

    async def send_rpc[R, A](
        self,
        service_name: str,
        procedure_name: str,
        request: R,
        request_serializer: Callable[[R], Any],
        response_deserializer: Callable[[Any], A],
        error_deserializer: Callable[[Any], RiverError],
        span: Span,
        timeout: timedelta,
    ) -> A:
        """Sends a single RPC request to the server.

        Expects the input and output be messages that will be msgpacked.
        """
        stream_id = nanoid.generate()
        await self._enqueue_message(
            stream_id=stream_id,
            control_flags=STREAM_OPEN_BIT | STREAM_CLOSED_BIT,
            payload=request_serializer(request),
            service_name=service_name,
            procedure_name=procedure_name,
            span=span,
        )

        async with self._with_stream(span, stream_id, 1) as (
            backpressured_waiter,
            output,
        ):
            # Handle potential errors during communication
            try:
                async with asyncio.timeout(timeout.total_seconds()):
                    # Block for backpressure and emission errors from the ws
                    await backpressured_waiter()
                    result = await anext(output)
            except asyncio.CancelledError:
                await self._send_cancel_stream(
                    stream_id=stream_id,
                    message="RPC cancelled",
                    span=span,
                )
                raise
            except asyncio.TimeoutError as e:
                await self._send_cancel_stream(
                    stream_id=stream_id,
                    message="Timeout, abandoning request",
                    span=span,
                )
                raise RiverException(ERROR_CODE_CANCEL, str(e)) from e
            except ChannelClosed as e:
                raise RiverServiceException(
                    ERROR_CODE_STREAM_CLOSED,
                    "Stream closed before response",
                    service_name,
                    procedure_name,
                ) from e

            if "ok" not in result or not result["ok"]:
                try:
                    error = error_deserializer(result["payload"])
                except Exception as e:
                    raise RiverException("error_deserializer", str(e)) from e
                raise exception_from_message(error.code)(
                    error.code, error.message, service_name, procedure_name
                )

            return response_deserializer(result["payload"])

    async def send_upload[I, R, A](
        self,
        service_name: str,
        procedure_name: str,
        init: I,
        request: AsyncIterable[R],
        init_serializer: Callable[[I], Any],
        request_serializer: Callable[[R], Any],
        response_deserializer: Callable[[Any], A],
        error_deserializer: Callable[[Any], RiverError],
        span: Span,
    ) -> A:
        """Sends an upload request to the server.

        Expects the input and output be messages that will be msgpacked.
        """
        stream_id = nanoid.generate()
        await self._enqueue_message(
            stream_id=stream_id,
            control_flags=STREAM_OPEN_BIT,
            service_name=service_name,
            procedure_name=procedure_name,
            payload=init_serializer(init),
            span=span,
        )

        async with self._with_stream(span, stream_id, 1) as (
            backpressured_waiter,
            output,
        ):
            try:
                # If this request is not closed and the session is killed, we should
                # throw exception here
                async for item in request:
                    # Block for backpressure
                    await backpressured_waiter()
                    try:
                        payload = request_serializer(item)
                    except Exception as e:
                        await self._send_cancel_stream(
                            stream_id=stream_id,
                            message="Request serialization error",
                            span=span,
                        )
                        raise RiverServiceException(
                            ERROR_CODE_STREAM_CLOSED,
                            str(e),
                            service_name,
                            procedure_name,
                        ) from e
                    await self._enqueue_message(
                        stream_id=stream_id,
                        service_name=service_name,
                        procedure_name=procedure_name,
                        control_flags=0,
                        payload=payload,
                        span=span,
                    )
            except asyncio.CancelledError:
                await self._send_cancel_stream(
                    stream_id=stream_id,
                    message="Upload cancelled",
                    span=span,
                )
                raise
            except Exception as e:
                # If we get any exception other than WebsocketClosedException,
                # cancel the stream.
                await self._send_cancel_stream(
                    stream_id=stream_id,
                    message="Unspecified error",
                    span=span,
                )
                raise RiverServiceException(
                    ERROR_CODE_STREAM_CLOSED, str(e), service_name, procedure_name
                ) from e
            await self._send_close_stream(
                stream_id=stream_id,
                span=span,
            )

            try:
                result = await anext(output)
            except ChannelClosed as e:
                raise RiverServiceException(
                    ERROR_CODE_STREAM_CLOSED,
                    "Stream closed before response",
                    service_name,
                    procedure_name,
                ) from e
            except Exception as e:
                await self._send_cancel_stream(
                    stream_id=stream_id,
                    message="Unspecified error",
                    span=span,
                )
                raise RiverException(ERROR_CODE_STREAM_CLOSED, str(e)) from e

            if "ok" not in result or not result["ok"]:
                try:
                    error = error_deserializer(result["payload"])
                except Exception as e:
                    raise RiverException("error_deserializer", str(e)) from e
                raise exception_from_message(error.code)(
                    error.code, error.message, service_name, procedure_name
                )

            return response_deserializer(result["payload"])

    async def send_subscription[I, E, A](
        self,
        service_name: str,
        procedure_name: str,
        init: I,
        init_serializer: Callable[[I], Any],
        response_deserializer: Callable[[Any], A],
        error_deserializer: Callable[[Any], E],
        span: Span,
    ) -> AsyncGenerator[A | E, None]:
        """Sends a subscription request to the server.

        Expects the input and output be messages that will be msgpacked.
        """
        stream_id = nanoid.generate()
        await self._enqueue_message(
            service_name=service_name,
            procedure_name=procedure_name,
            stream_id=stream_id,
            control_flags=STREAM_OPEN_BIT,
            payload=init_serializer(init),
            span=span,
        )

        async with self._with_stream(span, stream_id, MAX_MESSAGE_BUFFER_SIZE) as (
            _,
            output,
        ):
            try:
                async for item in output:
                    if item.get("type") == "CLOSE":
                        break
                    if not item.get("ok", False):
                        yield error_deserializer(item["payload"])
                        continue
                    yield response_deserializer(item["payload"])
                await self._send_close_stream(stream_id, span)
            except asyncio.CancelledError:
                await self._send_cancel_stream(
                    stream_id=stream_id,
                    message="Subscription cancelled",
                    span=span,
                )
                raise
            except Exception as e:
                await self._send_cancel_stream(
                    stream_id=stream_id,
                    message="Unspecified error",
                    span=span,
                )
                raise RiverServiceException(
                    ERROR_CODE_STREAM_CLOSED,
                    "Stream closed before response",
                    service_name,
                    procedure_name,
                ) from e

    async def send_stream[I, R, E, A](
        self,
        service_name: str,
        procedure_name: str,
        init: I,
        request: AsyncIterable[R] | None,
        init_serializer: Callable[[I], Any],
        request_serializer: Callable[[R], Any] | None,
        response_deserializer: Callable[[Any], A],
        error_deserializer: Callable[[Any], E],
        span: Span,
    ) -> AsyncGenerator[A | E, None]:
        """Sends a subscription request to the server.

        Expects the input and output be messages that will be msgpacked.
        """

        stream_id = nanoid.generate()
        await self._enqueue_message(
            service_name=service_name,
            procedure_name=procedure_name,
            stream_id=stream_id,
            control_flags=STREAM_OPEN_BIT,
            payload=init_serializer(init),
            span=span,
        )

        async with self._with_stream(span, stream_id, MAX_MESSAGE_BUFFER_SIZE) as (
            backpressured_waiter,
            output,
        ):
            # Create the encoder task
            async def _encode_stream() -> None:
                if not request:
                    await self._send_close_stream(
                        stream_id=stream_id,
                        span=span,
                    )
                    return

                assert request_serializer, "send_stream missing request_serializer"

                async for item in request:
                    # Block for backpressure
                    await backpressured_waiter()

                    await self._enqueue_message(
                        stream_id=stream_id,
                        control_flags=0,
                        payload=request_serializer(item),
                    )
                await self._send_close_stream(
                    stream_id=stream_id,
                    span=span,
                )

            emitter_task = self._task_manager.create_task(_encode_stream())

            # Handle potential errors during communication
            try:
                async for result in output:
                    # Raise as early as we possibly can in case of an emission error
                    if emitter_task.done() and (err := emitter_task.exception()):
                        raise err
                    if result.get("type") == "CLOSE":
                        break
                    if "ok" not in result or not result["ok"]:
                        yield error_deserializer(result["payload"])
                        continue
                    yield response_deserializer(result["payload"])
                # ... block the outer function until the emitter is finished emitting,
                #     possibly raising a terminal exception.
                await emitter_task
            except asyncio.CancelledError as e:
                await self._send_cancel_stream(
                    stream_id=stream_id,
                    message="Stream cancelled",
                    span=span,
                )
                if emitter_task.done() and (err := emitter_task.exception()):
                    raise e from err
                raise
            except Exception as e:
                await self._send_cancel_stream(
                    stream_id=stream_id,
                    message="Unspecified error",
                    span=span,
                )
                raise RiverServiceException(
                    ERROR_CODE_STREAM_CLOSED,
                    "Stream closed before response",
                    service_name,
                    procedure_name,
                ) from e

    async def _send_cancel_stream(
        self,
        stream_id: str,
        message: str,
        span: Span,
    ) -> None:
        await self._enqueue_message(
            stream_id=stream_id,
            control_flags=STREAM_CANCEL_BIT,
            payload={
                "ok": False,
                "payload": {
                    "code": "CANCEL",
                    "message": message,
                },
            },
            span=span,
        )

    async def _send_close_stream(
        self,
        stream_id: str,
        span: Span,
    ) -> None:
        await self._enqueue_message(
            stream_id=stream_id,
            control_flags=STREAM_CLOSED_BIT,
            payload={"type": "CLOSE"},
            span=span,
        )


async def _do_ensure_connected[HandshakeMetadata](
    transport_options: TransportOptions,
    client_id: str,
    session_id: str,
    server_id: str,
    rate_limiter: RateLimiter,
    uri_and_metadata_factory: Callable[
        [], Awaitable[UriAndMetadata[HandshakeMetadata]]
    ],
    get_current_time: Callable[[], Awaitable[float]],
    get_next_sent_seq: Callable[[], int],
    get_current_ack: Callable[[], int],
    get_state: Callable[[], SessionState],
    transition_connecting: Callable[[ClientConnection], None],
    close_ws_in_background: Callable[[ClientConnection], None],
    transition_connected: Callable[[ClientConnection], None],
    unbind_connecting_task: Callable[[], None],
    close_session: Callable[[Exception | None], None],
) -> None:
    logger.info("Attempting to establish new ws connection")

    last_error: Exception | None = None
    attempt_count = 0
    while rate_limiter.has_budget(client_id):
        if (state := get_state()) in TerminalStates or state in ActiveStates:
            logger.info(f"_do_ensure_connected stopping due to state={state}")
            break

        if attempt_count > 0:
            logger.info(f"Retrying build handshake number {attempt_count} times")
        attempt_count += 1

        rate_limiter.consume_budget(client_id)

        ws: ClientConnection | None = None
        try:
            uri_and_metadata = await uri_and_metadata_factory()
            ws = await websockets.asyncio.client.connect(
                uri_and_metadata["uri"],
                max_size=None,
            )
            transition_connecting(ws)

            try:
                handshake_request = ControlMessageHandshakeRequest[HandshakeMetadata](
                    type="HANDSHAKE_REQ",
                    protocolVersion=PROTOCOL_VERSION,
                    sessionId=session_id,
                    metadata=uri_and_metadata["metadata"],
                    expectedSessionState=ExpectedSessionState(
                        nextExpectedSeq=get_current_ack(),
                        nextSentSeq=get_next_sent_seq(),
                    ),
                )

                async def websocket_closed_callback() -> None:
                    logger.error("websocket closed before handshake response")

                await send_transport_message(
                    TransportMessage(
                        from_=client_id,
                        to=server_id,
                        streamId=nanoid.generate(),
                        controlFlags=0,
                        id=nanoid.generate(),
                        seq=0,
                        ack=0,
                        payload=handshake_request.model_dump(),
                    ),
                    ws=ws,
                    websocket_closed_callback=websocket_closed_callback,
                )
            except (
                WebsocketClosedException,
                FailedSendingMessageException,
            ) as e:
                raise RiverException(
                    ERROR_HANDSHAKE,
                    "Handshake failed, conn closed while sending response",
                ) from e

            handshake_deadline_ms = (
                await get_current_time() + transport_options.handshake_timeout_ms
            )

            if await get_current_time() >= handshake_deadline_ms:
                raise RiverException(
                    ERROR_HANDSHAKE,
                    "Handshake response timeout, closing connection",
                )

            try:
                data = await ws.recv(decode=False)
            except ConnectionClosedOK:
                # In the case of a normal connection closure, we defer to
                # the outer loop to determine next steps.
                # A call to close(...) should set the SessionState to a terminal one,
                # otherwise we should try again.
                continue
            except ConnectionClosed as e:
                logger.debug(
                    "_do_ensure_connected: Connection closed during waiting "
                    "for handshake response",
                    exc_info=True,
                )
                raise RiverException(
                    ERROR_HANDSHAKE,
                    "Handshake failed, conn closed while waiting for response",
                ) from e

            try:
                response_msg = parse_transport_msg(data)
            except InvalidMessageException as e:
                raise RiverException(
                    ERROR_HANDSHAKE,
                    "Got invalid transport message, closing connection",
                ) from e

            if isinstance(response_msg, str):
                raise RiverException(
                    ERROR_HANDSHAKE,
                    "Handshake failed, received a raw string message while waiting "
                    "for a handshake response",
                )

            try:
                handshake_response = ControlMessageHandshakeResponse(
                    **response_msg.payload
                )
                logger.debug("river client waiting for handshake response")
            except ValidationError as e:
                raise RiverException(
                    ERROR_HANDSHAKE, "Failed to parse handshake response"
                ) from e

            logger.debug("river client get handshake response : %r", handshake_response)
            if not handshake_response.status.ok:
                err = RiverException(
                    ERROR_HANDSHAKE,
                    f"Handshake failed with code {handshake_response.status.code}: {
                        handshake_response.status.reason
                    }",
                )
                if handshake_response.status.code == ERROR_CODE_SESSION_STATE_MISMATCH:
                    # A session state mismatch is unrecoverable. Terminate immediately.
                    close_session(err)

                raise err

            logger.debug("Connected")
            # We did it! We're connected!
            last_error = None
            rate_limiter.start_restoring_budget(client_id)
            transition_connected(ws)
            break
        except Exception as e:
            backoff_time = rate_limiter.get_backoff_ms(client_id)
            logger.exception(
                f"Error connecting, retrying with {backoff_time}ms backoff"
            )
            if ws:
                close_ws_in_background(ws)
                ws = None
            last_error = e
            await asyncio.sleep(backoff_time / 1000)
        logger.debug("Here, about to retry")
    unbind_connecting_task()

    if last_error is not None:
        logger.debug("Handshake attempts exhausted, terminating")
        close_session(last_error)
        raise RiverException(
            ERROR_HANDSHAKE,
            f"Failed to create ws after retrying {attempt_count} number of times",
        ) from last_error

    return None


async def _recv_from_ws(
    block_until_connected: Callable[[], Awaitable[None]],
    client_id: str,
    get_state: Callable[[], SessionState],
    get_ws: Callable[[], ClientConnection | None],
    transition_no_connection: Callable[[], Awaitable[None]],
    close_session: Callable[[Exception | None], None],
    assert_incoming_seq_bookkeeping: Callable[
        [str, int, int], Literal[True] | _IgnoreMessage
    ],
    get_stream: Callable[
        [str],
        StreamMeta | None,
    ],
    enqueue_message: SendMessage[None],
) -> None:
    """Serve messages from the websocket.

    Process incoming packets from the connected websocket.
    """
    our_task = asyncio.current_task()
    connection_attempts = 0
    while our_task and not our_task.cancelling() and not our_task.cancelled():
        try:
            logger.debug(f"_recv_from_ws loop count={connection_attempts}")
            connection_attempts += 1
            ws = None
            while ((state := get_state()) in ConnectingStates) and (
                state not in TerminalStates
            ):
                logger.debug(
                    "_handle_messages_from_ws spinning while connecting, %r %r",
                    ws,
                    state,
                )
                await block_until_connected()

            if state in TerminalStates:
                logger.debug(
                    f"Session is {state}, shut down _recv_from_ws",
                )
                # session is closing / closed, no need to _recv_from_ws anymore
                break

            logger.debug("client start handling messages from ws %r", ws)

            # We should not process messages if the websocket is closed.
            while (ws := get_ws()) and get_state() in ActiveStates:
                connection_attempts = 0
                # decode=False: Avoiding an unnecessary round-trip through str
                # Ideally this should be type-ascripted to : bytes, but there
                # is no @overrides in `websockets` to hint this.
                try:
                    message = await ws.recv(decode=False)
                except ConnectionClosedOK as e:
                    close_session(e)
                    continue
                except ConnectionClosed:
                    # This triggers a break in the inner loop so we can get back to
                    # the outer loop.
                    await transition_no_connection()
                    break
                msg: TransportMessage | str | None = None
                try:
                    msg = parse_transport_msg(message)
                    logger.debug(
                        "[%s] got a message %r",
                        client_id,
                        msg,
                    )
                    if isinstance(msg, str):
                        logger.debug("Ignoring transport message", exc_info=True)
                        continue

                    if msg.controlFlags & STREAM_OPEN_BIT != 0:
                        raise InvalidMessageException(
                            "Client should not receive stream open bit"
                        )

                    match assert_incoming_seq_bookkeeping(
                        msg.from_,
                        msg.seq,
                        msg.ack,
                    ):
                        case _IgnoreMessage():
                            logger.debug(
                                "Ignoring transport message",
                                exc_info=True,
                            )
                            continue
                        case True:
                            pass
                        case other:
                            assert_never(other)

                    # Shortcut to avoid processing ack packets
                    if msg.controlFlags & ACK_BIT != 0:
                        await enqueue_message(
                            stream_id="heartbeat",
                            # TODO: make this a message class
                            # https://github.com/replit/river/blob/741b1ea6d7600937ad53564e9cf8cd27a92ec36a/transport/message.ts#L42
                            payload={
                                "type": "ACK",
                            },
                            control_flags=ACK_BIT,
                            procedure_name=None,
                            service_name=None,
                            span=None,
                        )
                        continue

                    stream_meta = get_stream(msg.streamId)

                    if not stream_meta:
                        logger.warning(
                            "no stream for %s, ignoring message",
                            msg.streamId,
                        )
                        continue

                    if (
                        msg.controlFlags & STREAM_CLOSED_BIT != 0
                        and msg.payload.get("type", None) == "CLOSE"
                    ):
                        # close message is not sent to the stream
                        # event is set during cleanup down below
                        pass
                    else:
                        try:
                            await stream_meta["output"].put(msg.payload)
                        except ChannelClosed:
                            # The client is no longer interested in this stream,
                            # just drop the message.
                            pass

                    if msg.controlFlags & STREAM_CLOSED_BIT != 0:
                        # Communicate that we're going down
                        #
                        # This implements the receive side of the half-closed strategy.
                        stream_meta["output"].close()
                except OutOfOrderMessageException:
                    logger.exception("Out of order message, closing connection")
                    stream_id = "unknown"
                    if isinstance(msg, TransportMessage):
                        stream_id = msg.streamId
                    close_session(
                        SessionClosedRiverServiceException(
                            "Out of order message, closing connection",
                            stream_id,
                        )
                    )
                    continue
                except InvalidMessageException:
                    logger.exception(
                        "Got invalid transport message, closing session",
                    )
                    stream_id = "unknown"
                    if isinstance(msg, TransportMessage):
                        stream_id = msg.streamId
                    close_session(
                        SessionClosedRiverServiceException(
                            "Out of order message, closing connection",
                            stream_id,
                        )
                    )
                    continue
                except FailedSendingMessageException:
                    # Expected error if the connection is closed.
                    await transition_no_connection()
                    logger.debug(
                        "FailedSendingMessageException while serving", exc_info=True
                    )
                    break  # Inner loop
                except Exception:
                    logger.exception("caught exception at message iterator")
                    await transition_no_connection()
                    break  # Inner loop
            logger.debug("_handle_messages_from_ws exiting")
        except ExceptionGroup as eg:
            _, unhandled = eg.split(lambda e: isinstance(e, ConnectionClosed))
            if unhandled:
                # We're in a task, there's not that much that can be done.
                unhandled = ExceptionGroup(
                    "Unhandled exceptions on River server", unhandled.exceptions
                )
                logger.exception(
                    "caught exception at message iterator",
                    exc_info=unhandled,
                )
                raise unhandled
    logger.debug(f"_recv_from_ws exiting normally after {connection_attempts} loops")
