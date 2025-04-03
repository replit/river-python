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
from aiochannel import Channel
from aiochannel.errors import ChannelClosed
from opentelemetry.trace import Span, use_span
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from pydantic import ValidationError
from websockets.asyncio.client import ClientConnection
from websockets.exceptions import ConnectionClosed, ConnectionClosedOK
from websockets.protocol import CLOSED

from replit_river.common_session import (
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
    StreamClosedRiverServiceException,
    exception_from_message,
)
from replit_river.messages import (
    FailedSendingMessageException,
    WebsocketClosedException,
    parse_transport_msg,
    send_transport_message,
)
from replit_river.rate_limiter import LeakyBucketRateLimit
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

CloseSessionCallback: TypeAlias = Callable[["Session"], Coroutine[Any, Any, Any]]
RetryConnectionCallback: TypeAlias = Callable[
    [],
    Coroutine[Any, Any, Any],
]


@dataclass
class _IgnoreMessage:
    pass


class Session[HandshakeMetadata]:
    _transport_id: str
    _to_id: str
    session_id: str
    _transport_options: TransportOptions

    # session state, only modified during closing
    _state: SessionState
    _close_session_callback: CloseSessionCallback
    _close_session_after_time_secs: float | None
    _connecting_task: asyncio.Task[None] | None
    _wait_for_connected: asyncio.Event

    _client_id: str
    _rate_limiter: LeakyBucketRateLimit
    _uri_and_metadata_factory: Callable[
        [], Awaitable[UriAndMetadata[HandshakeMetadata]]
    ]

    # ws state
    _ws: ClientConnection | None
    _heartbeat_misses: int
    _retry_connection_callback: RetryConnectionCallback | None

    # message state
    _process_messages: asyncio.Event
    _space_available: asyncio.Event

    # stream for tasks
    _streams: dict[str, tuple[asyncio.Event, Channel[Any]]]

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
        transport_id: str,
        to_id: str,
        session_id: str,
        transport_options: TransportOptions,
        close_session_callback: CloseSessionCallback,
        client_id: str,
        rate_limiter: LeakyBucketRateLimit,
        uri_and_metadata_factory: Callable[
            [], Awaitable[UriAndMetadata[HandshakeMetadata]]
        ],
        retry_connection_callback: RetryConnectionCallback | None = None,
    ) -> None:
        self._transport_id = transport_id
        self._to_id = to_id
        self.session_id = session_id
        self._transport_options = transport_options

        # session state
        self._state = SessionState.NO_CONNECTION
        self._close_session_callback = close_session_callback
        self._close_session_after_time_secs: float | None = None
        self._connecting_task = None
        self._wait_for_connected = asyncio.Event()

        self._client_id = client_id
        self._rate_limiter = rate_limiter
        self._uri_and_metadata_factory = uri_and_metadata_factory

        # ws state
        self._ws = None
        self._heartbeat_misses = 0
        self._retry_connection_callback = retry_connection_callback

        # message state
        self._process_messages = asyncio.Event()
        self._space_available = asyncio.Event()
        # Ensure we initialize the above Event to "set" to avoid being blocked from
        # the beginning.
        self._space_available.set()

        # stream for tasks
        self._streams: dict[str, tuple[asyncio.Event, Channel[Any]]] = {}

        # book keeping
        self._ack_buffer = deque()
        self._send_buffer = deque()
        self._task_manager = BackgroundTaskManager()
        self.ack = 0
        self.seq = 0

        # Terminating
        self._terminating_task = None

        self._start_serve_responses()
        self._start_close_session_checker()
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

        def do_close() -> None:
            # Avoid closing twice
            if self._terminating_task is None:
                # We can't just call self.close() directly because
                # we're inside a thread that will eventually be awaited
                # during the cleanup procedure.
                self._terminating_task = asyncio.create_task(self.close())

        def transition_connecting() -> None:
            if self._state in TerminalStates:
                return
            logger.debug("transition_connecting")
            self._state = SessionState.CONNECTING
            # "Clear" here means observers should wait until we are connected.
            self._wait_for_connected.clear()

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

        def finalize_attempt() -> None:
            # We are in a state where we may throw an exception.
            #
            # To allow subsequent calls to ensure_connected to pass, we clear ourselves.
            # This is safe because each individual function that is waiting on this
            # function completeing already has a reference, so we'll last a few ticks
            # before GC.
            #
            # Let's do our best to avoid clobbering other tasks by comparing the .name
            current_task = asyncio.current_task()
            if (
                self._connecting_task
                and current_task
                and self._connecting_task is current_task
            ):
                self._connecting_task = None

        if not self._connecting_task:
            self._connecting_task = asyncio.create_task(
                _do_ensure_connected(
                    transport_id=self._transport_id,
                    client_id=self._client_id,
                    to_id=self._to_id,
                    session_id=self.session_id,
                    max_retry=self._transport_options.connection_retry_options.max_retry,
                    rate_limiter=self._rate_limiter,
                    uri_and_metadata_factory=self._uri_and_metadata_factory,
                    get_next_sent_seq=get_next_sent_seq,
                    get_current_ack=lambda: self.ack,
                    get_current_time=self._get_current_time,
                    transition_connecting=transition_connecting,
                    close_ws_in_background=close_ws_in_background,
                    transition_connected=transition_connected,
                    finalize_attempt=finalize_attempt,
                    do_close=do_close,
                )
            )

        await self._connecting_task

    def is_closed(self) -> bool:
        """
        If the session is in a terminal state.
        Do not send messages, do not expect any more messages to be emitted,
        the state is expected to be stale.
        """
        return self._state in TerminalStates

    def is_connected(self) -> bool:
        return self._state == SessionState.ACTIVE

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
        self._state = SessionState.NO_CONNECTION
        self._close_session_after_time_secs = close_session_after_time_secs
        self._wait_for_connected.clear()

    async def _get_current_time(self) -> float:
        return asyncio.get_event_loop().time()

    def _reset_session_close_countdown(self) -> None:
        logger.debug("_reset_session_close_countdown")
        self._heartbeat_misses = 0
        self._close_session_after_time_secs = None

    async def _send_message(
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
        if self._state in TerminalStates:
            return
        logger.debug(
            "_send_message(stream_id=%r, payload=%r, control_flags=%r, "
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
            )

        # Begin critical section: Avoid any await between here and _send_buffer.append
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

        # We're clear to add to the send buffer
        self._send_buffer.append(msg)

        # Increment immediately so we maintain consistency
        self.seq += 1

        # If the buffer is now full, reset the block
        if len(self._send_buffer) >= self._transport_options.buffer_size:
            self._space_available.clear()

        # Wake up buffered_message_sender
        self._process_messages.set()

    async def close(self) -> None:
        """Close the session and all associated streams."""
        logger.info(
            f"{self._transport_id} closing session to {self._to_id}, ws: {self._ws}"
        )
        if self._state in TerminalStates:
            # already closing
            return
        self._state = SessionState.CLOSING

        # We're closing, so we need to wake up...
        # ... tasks waiting for connection to be established
        self._wait_for_connected.set()
        # ... consumers waiting to enqueue messages
        self._space_available.set()
        # ... message processor so it can exit cleanly
        self._process_messages.set()

        # Wait a tick to permit the waiting tasks to shut down gracefully
        await asyncio.sleep(0.01)

        await self._task_manager.cancel_all_tasks()

        # TODO: unexpected_close should close stream differently here to
        # throw exception correctly.
        for backpressure_waiter, stream in self._streams.values():
            stream.close()
            # Wake up backpressured writers
            backpressure_waiter.set()
        # Before we GC the streams, let's wait for all tasks to be closed gracefully.
        await asyncio.gather(*[stream.join() for _, stream in self._streams.values()])
        self._streams.clear()

        if self._ws:
            # The Session isn't guaranteed to live much longer than this close()
            # invocation, so let's await this close to avoid dropping the socket.
            await self._ws.close()

        self._state = SessionState.CLOSED

        # Clear the session in transports
        # This will get us GC'd, so this should be the last thing.
        await self._close_session_callback(self)

    def _start_buffered_message_sender(self) -> None:
        def commit(msg: TransportMessage) -> None:
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
                stream_meta[0].set()

        def get_next_pending() -> TransportMessage | None:
            if self._send_buffer:
                return self._send_buffer[0]
            return None

        def get_ws() -> ClientConnection | None:
            if self.is_connected():
                return self._ws
            return None

        async def block_until_connected() -> None:
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
                websocket_closed_callback=self._begin_close_session_countdown,
                get_next_pending=get_next_pending,
                commit=commit,
                get_state=lambda: self._state,
            )
        )

    def _start_close_session_checker(self) -> None:
        def transition_connecting() -> None:
            if self._state in TerminalStates:
                return
            self._state = SessionState.CONNECTING
            self._wait_for_connected.clear()

        self._task_manager.create_task(
            _check_to_close_session(
                self._transport_options.close_session_check_interval_ms,
                lambda: self._state,
                lambda: self._ws,
                transition_connecting=transition_connecting,
            )
        )

    def _start_serve_responses(self) -> None:
        def transition_connecting() -> None:
            if self._state in TerminalStates:
                return
            self._state = SessionState.CONNECTING
            self._wait_for_connected.clear()

        async def transition_no_connection() -> None:
            if self._state in TerminalStates:
                return
            self._state = SessionState.NO_CONNECTION
            if self._ws:
                self._task_manager.create_task(self._ws.close())
                self._ws = None

            if self._retry_connection_callback:
                self._task_manager.create_task(self._retry_connection_callback())

            await self._begin_close_session_countdown()

        def assert_incoming_seq_bookkeeping(
            msg_from: str,
            msg_seq: int,
            msg_ack: int,
        ) -> Literal[True] | _IgnoreMessage:
            # Update bookkeeping
            if msg_seq < self.ack:
                logger.info(
                    f"{msg_from} received duplicate msg, got {msg_seq}"
                    f" expected {self.ack}"
                )
                return _IgnoreMessage()
            elif msg_seq > self.ack:
                logger.warning(
                    f"Out of order message received got {msg_seq} expected {self.ack}"
                )

                raise OutOfOrderMessageException(
                    f"Out of order message received got {msg_seq} expected {self.ack}"
                )

            assert msg_seq == self.ack, "Safety net, redundant assertion"

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
            _serve(
                block_until_connected=block_until_connected,
                transport_id=self._transport_id,
                get_state=lambda: self._state,
                get_ws=lambda: self._ws,
                transition_connecting=transition_connecting,
                transition_no_connection=transition_no_connection,
                reset_session_close_countdown=self._reset_session_close_countdown,
                close_session=self.close,
                assert_incoming_seq_bookkeeping=assert_incoming_seq_bookkeeping,
                get_stream=lambda stream_id: self._streams.get(stream_id),
                send_message=self._send_message,
            )
        )

    @asynccontextmanager
    async def _with_stream(
        self,
        session_id: str,
        maxsize: int,
    ) -> AsyncIterator[tuple[asyncio.Event, Channel[ResultType]]]:
        output: Channel[Any] = Channel(maxsize=maxsize)
        backpressure_waiter = asyncio.Event()
        self._streams[session_id] = (backpressure_waiter, output)
        try:
            yield (backpressure_waiter, output)
        finally:
            del self._streams[session_id]

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
        async with self._with_stream(stream_id, 1) as (backpressure_waiter, output):
            await self._send_message(
                stream_id=stream_id,
                control_flags=STREAM_OPEN_BIT | STREAM_CLOSED_BIT,
                payload=request_serializer(request),
                service_name=service_name,
                procedure_name=procedure_name,
                span=span,
            )
            # Handle potential errors during communication
            try:
                async with asyncio.timeout(timeout.total_seconds()):
                    # Block for event for symmetry with backpressured producers
                    # Here this should be trivially true.
                    await backpressure_waiter.wait()
                    result = await output.get()
            except asyncio.TimeoutError as e:
                await self._send_cancel_stream(
                    stream_id=stream_id,
                    extra_control_flags=0,
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
            except RuntimeError as e:
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

    async def send_upload[I, R, A](
        self,
        service_name: str,
        procedure_name: str,
        init: I,
        request: AsyncIterable[R] | None,
        init_serializer: Callable[[I], Any],
        request_serializer: Callable[[R], Any] | None,
        response_deserializer: Callable[[Any], A],
        error_deserializer: Callable[[Any], RiverError],
        span: Span,
    ) -> A:
        """Sends an upload request to the server.

        Expects the input and output be messages that will be msgpacked.
        """

        stream_id = nanoid.generate()
        async with self._with_stream(stream_id, 1) as (backpressure_waiter, output):
            try:
                await self._send_message(
                    stream_id=stream_id,
                    control_flags=STREAM_OPEN_BIT,
                    service_name=service_name,
                    procedure_name=procedure_name,
                    payload=init_serializer(init),
                    span=span,
                )

                if request:
                    assert request_serializer, "send_stream missing request_serializer"

                    # If this request is not closed and the session is killed, we should
                    # throw exception here
                    async for item in request:
                        # Block for backpressure
                        await backpressure_waiter.wait()
                        if output.closed():
                            logger.debug("Stream is closed, avoid sending the rest")
                            break
                        await self._send_message(
                            stream_id=stream_id,
                            service_name=service_name,
                            procedure_name=procedure_name,
                            control_flags=0,
                            payload=request_serializer(item),
                            span=span,
                        )
            except WebsocketClosedException as e:
                raise RiverServiceException(
                    ERROR_CODE_STREAM_CLOSED, str(e), service_name, procedure_name
                ) from e
            except Exception as e:
                # If we get any exception other than WebsocketClosedException,
                # cancel the stream.
                await self._send_cancel_stream(
                    stream_id=stream_id,
                    extra_control_flags=0,
                    span=span,
                )
                raise RiverServiceException(
                    ERROR_CODE_STREAM_CLOSED, str(e), service_name, procedure_name
                ) from e
            await self._send_close_stream(
                stream_id=stream_id,
                extra_control_flags=0,
                span=span,
            )

            # Handle potential errors during communication
            # TODO: throw a error when the transport is hard closed
            try:
                result = await output.get()
            except ChannelClosed as e:
                raise RiverServiceException(
                    ERROR_CODE_STREAM_CLOSED,
                    "Stream closed before response",
                    service_name,
                    procedure_name,
                ) from e
            except RuntimeError as e:
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

    async def send_subscription[R, E, A](
        self,
        service_name: str,
        procedure_name: str,
        request: R,
        request_serializer: Callable[[R], Any],
        response_deserializer: Callable[[Any], A],
        error_deserializer: Callable[[Any], E],
        span: Span,
    ) -> AsyncGenerator[A | E, None]:
        """Sends a subscription request to the server.

        Expects the input and output be messages that will be msgpacked.
        """
        stream_id = nanoid.generate()
        async with self._with_stream(stream_id, MAX_MESSAGE_BUFFER_SIZE) as (_, output):
            await self._send_message(
                service_name=service_name,
                procedure_name=procedure_name,
                stream_id=stream_id,
                control_flags=STREAM_OPEN_BIT,
                payload=request_serializer(request),
                span=span,
            )

            # Handle potential errors during communication
            try:
                async for item in output:
                    if item.get("type") == "CLOSE":
                        break
                    if not item.get("ok", False):
                        try:
                            yield error_deserializer(item["payload"])
                        except Exception:
                            logger.exception(
                                "Error during subscription "
                                f"error deserialization: {item}"
                            )
                        continue
                    yield response_deserializer(item["payload"])
            except (RuntimeError, ChannelClosed) as e:
                raise RiverServiceException(
                    ERROR_CODE_STREAM_CLOSED,
                    "Stream closed before response",
                    service_name,
                    procedure_name,
                ) from e
            except Exception as e:
                raise e
            finally:
                output.close()

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
        async with self._with_stream(
            stream_id,
            MAX_MESSAGE_BUFFER_SIZE,
        ) as (backpressure_waiter, output):
            try:
                await self._send_message(
                    service_name=service_name,
                    procedure_name=procedure_name,
                    stream_id=stream_id,
                    control_flags=STREAM_OPEN_BIT,
                    payload=init_serializer(init),
                    span=span,
                )
            except Exception as e:
                raise StreamClosedRiverServiceException(
                    ERROR_CODE_STREAM_CLOSED, str(e), service_name, procedure_name
                ) from e

            # Create the encoder task
            async def _encode_stream() -> None:
                if not request:
                    await self._send_close_stream(
                        stream_id=stream_id,
                        extra_control_flags=STREAM_OPEN_BIT,
                        span=span,
                    )
                    return

                assert request_serializer, "send_stream missing request_serializer"

                async for item in request:
                    if item is None:
                        continue
                    await backpressure_waiter.wait()
                    if output.closed():
                        logger.debug("Stream is closed, avoid sending the rest")
                        break
                    await self._send_message(
                        stream_id=stream_id,
                        control_flags=0,
                        payload=request_serializer(item),
                    )
                await self._send_close_stream(
                    stream_id=stream_id,
                    extra_control_flags=0,
                    span=span,
                )

            self._task_manager.create_task(_encode_stream())

            # Handle potential errors during communication
            try:
                async for result in output:
                    if result.get("type") == "CLOSE":
                        break
                    if "ok" not in result or not result["ok"]:
                        try:
                            yield error_deserializer(result["payload"])
                        except Exception:
                            logger.exception(
                                f"Error during stream error deserialization: {result}"
                            )
                        continue
                    yield response_deserializer(result["payload"])
            except (RuntimeError, ChannelClosed) as e:
                raise RiverServiceException(
                    ERROR_CODE_STREAM_CLOSED,
                    "Stream closed before response",
                    service_name,
                    procedure_name,
                ) from e
            except Exception as e:
                raise e
            finally:
                output.close()
                backpressure_waiter.set()

    async def _send_cancel_stream(
        self,
        stream_id: str,
        extra_control_flags: int,
        span: Span,
    ) -> None:
        await self._send_message(
            stream_id=stream_id,
            control_flags=STREAM_CANCEL_BIT | extra_control_flags,
            payload={"type": "CANCEL"},
            span=span,
        )

    async def _send_close_stream(
        self,
        stream_id: str,
        extra_control_flags: int,
        span: Span,
    ) -> None:
        await self._send_message(
            stream_id=stream_id,
            control_flags=STREAM_CLOSED_BIT | extra_control_flags,
            payload={"type": "CLOSE"},
            span=span,
        )


async def _check_to_close_session(
    close_session_check_interval_ms: float,
    get_state: Callable[[], SessionState],
    get_ws: Callable[[], ClientConnection | None],
    transition_connecting: Callable[[], None],
) -> None:
    while get_state() not in TerminalStates:
        logger.debug("_check_to_close_session: Checking")
        await asyncio.sleep(close_session_check_interval_ms / 1000)

        if (ws := get_ws()) and ws.protocol.state is CLOSED:
            logger.info("Websocket is closed, transitioning to connecting")
            transition_connecting()


async def _do_ensure_connected[HandshakeMetadata](
    transport_id: str,
    client_id: str,
    to_id: str,
    session_id: str,
    max_retry: int,
    rate_limiter: LeakyBucketRateLimit,
    uri_and_metadata_factory: Callable[
        [], Awaitable[UriAndMetadata[HandshakeMetadata]]
    ],
    get_current_time: Callable[[], Awaitable[float]],
    get_next_sent_seq: Callable[[], int],
    get_current_ack: Callable[[], int],
    transition_connecting: Callable[[], None],
    close_ws_in_background: Callable[[ClientConnection], None],
    transition_connected: Callable[[ClientConnection], None],
    finalize_attempt: Callable[[], None],
    do_close: Callable[[], None],
) -> None:
    logger.info("Attempting to establish new ws connection")

    last_error: Exception | None = None
    i = 0
    while rate_limiter.has_budget(client_id):
        if i > 0:
            logger.info(f"Retrying build handshake number {i} times")
        i += 1

        rate_limiter.consume_budget(client_id)
        transition_connecting()

        ws: ClientConnection | None = None
        try:
            uri_and_metadata = await uri_and_metadata_factory()
            ws = await websockets.asyncio.client.connect(uri_and_metadata["uri"])

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
                        from_=transport_id,
                        to=to_id,
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

            startup_grace_deadline_ms = await get_current_time() + 60_000
            while True:
                if await get_current_time() >= startup_grace_deadline_ms:
                    raise RiverException(
                        ERROR_HANDSHAKE,
                        "Handshake response timeout, closing connection",
                    )
                try:
                    data = await ws.recv(decode=False)
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
                    if isinstance(response_msg, str):
                        logger.debug(
                            "_do_ensure_connected: Ignoring transport message",
                            exc_info=True,
                        )
                        continue

                    break
                except InvalidMessageException as e:
                    raise RiverException(
                        ERROR_HANDSHAKE,
                        "Got invalid transport message, closing connection",
                    ) from e

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
                if handshake_response.status.code == ERROR_CODE_SESSION_STATE_MISMATCH:
                    do_close()

                raise RiverException(
                    ERROR_HANDSHAKE,
                    f"Handshake failed with code {handshake_response.status.code}: {
                        handshake_response.status.reason
                    }",
                )

            # We did it! We're connected!
            last_error = None
            rate_limiter.start_restoring_budget(client_id)
            transition_connected(ws)
            break
        except Exception as e:
            if ws:
                close_ws_in_background(ws)
                ws = None
            last_error = e
            backoff_time = rate_limiter.get_backoff_ms(client_id)
            logger.exception(
                f"Error connecting, retrying with {backoff_time}ms backoff"
            )
            await asyncio.sleep(backoff_time / 1000)
    finalize_attempt()

    if last_error is not None:
        logger.debug("Handshake attempts exhausted, terminating")
        do_close()
        raise RiverException(
            ERROR_HANDSHAKE,
            f"Failed to create ws after retrying {max_retry} number of times",
        ) from last_error

    return None


async def _serve(
    block_until_connected: Callable[[], Awaitable[None]],
    transport_id: str,
    get_state: Callable[[], SessionState],
    get_ws: Callable[[], ClientConnection | None],
    transition_connecting: Callable[[], None],
    transition_no_connection: Callable[[], Awaitable[None]],
    reset_session_close_countdown: Callable[[], None],
    close_session: Callable[[], Awaitable[None]],
    assert_incoming_seq_bookkeeping: Callable[
        [str, int, int], Literal[True] | _IgnoreMessage
    ],
    get_stream: Callable[[str], tuple[asyncio.Event, Channel[Any]] | None],
    send_message: SendMessage[None],
) -> None:
    """Serve messages from the websocket."""
    reset_session_close_countdown()
    our_task = asyncio.current_task()
    idx = 0
    try:
        while our_task and not our_task.cancelling() and not our_task.cancelled():
            logger.debug(f"_serve loop count={idx}")
            idx += 1
            ws = None
            while (state := get_state()) in ConnectingStates or (
                ws := get_ws()
            ) is None:
                logger.debug(
                    "_handle_messages_from_ws spinning while connecting, %r %r",
                    ws,
                    state,
                )
                await block_until_connected()
                if state in TerminalStates:
                    break

            if state in TerminalStates:
                logger.debug(
                    f"Session is {state}, shut down _serve",
                )
                # session is closing / closed, no need to serve anymore
                break

            # This should not happen, but due to the complex logic around TerminalStates
            # above, pyright is not convinced we've caught all the states.
            if not ws:
                continue

            logger.debug(
                "%s start handling messages from ws %s",
                "client",
                ws.id,
            )
            # We should not process messages if the websocket is closed.
            while (ws := get_ws()) and get_state() == SessionState.ACTIVE:
                # decode=False: Avoiding an unnecessary round-trip through str
                # Ideally this should be type-ascripted to : bytes, but there
                # is no @overrides in `websockets` to hint this.
                try:
                    message = await ws.recv(decode=False)
                except ConnectionClosed:
                    transition_connecting()
                    continue
                try:
                    msg = parse_transport_msg(message)
                    logger.debug(
                        "[%s] got a message %r",
                        transport_id,
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

                    reset_session_close_countdown()

                    # Shortcut to avoid processing ack packets
                    if msg.controlFlags & ACK_BIT != 0:
                        await send_message(
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

                    event_stream = get_stream(msg.streamId)

                    if not event_stream:
                        logger.warning(
                            "no stream for %s, ignoring message",
                            msg.streamId,
                        )
                        continue

                    backpressure_waiter, stream = event_stream

                    if (
                        msg.controlFlags & STREAM_CLOSED_BIT != 0
                        and msg.payload.get("type", None) == "CLOSE"
                    ):
                        # close message is not sent to the stream
                        # event is set during cleanup down below
                        pass
                    else:
                        try:
                            await stream.put(msg.payload)
                        except ChannelClosed:
                            # The client is no longer interested in this stream,
                            # just drop the message.
                            pass
                        except RuntimeError as e:
                            raise InvalidMessageException(e) from e

                    if msg.controlFlags & STREAM_CLOSED_BIT != 0:
                        # Communicate that we're going down
                        stream.close()
                        # Wake up backpressured writer
                        backpressure_waiter.set()
                except OutOfOrderMessageException:
                    logger.exception("Out of order message, closing connection")
                    await close_session()
                    continue
                except InvalidMessageException:
                    logger.exception(
                        "Got invalid transport message, closing session",
                    )
                    await close_session()
                    continue
                except ConnectionClosedOK:
                    # Exited normally
                    transition_connecting()
                    break
                except ConnectionClosed:
                    # Set ourselves to closed as soon as we get the signal
                    await transition_no_connection()
                    logger.debug("ConnectionClosed while serving", exc_info=True)
                    break
                except FailedSendingMessageException:
                    # Expected error if the connection is closed.
                    await transition_no_connection()
                    logger.debug(
                        "FailedSendingMessageException while serving", exc_info=True
                    )
                    break
                except Exception:
                    logger.exception("caught exception at message iterator")
                    break
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
    logger.debug(f"_serve exiting normally after {idx} loops")
