import asyncio
import logging
from collections import deque
from collections.abc import AsyncIterable
from datetime import timedelta
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Coroutine,
    Literal,
    TypeAlias,
)

import nanoid  # type: ignore
import websockets.asyncio.client
from aiochannel import Channel
from aiochannel.errors import ChannelClosed
from opentelemetry.trace import Span, use_span
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from pydantic import ValidationError
from websockets.asyncio.client import ClientConnection
from websockets.exceptions import ConnectionClosed, ConnectionClosedOK
from websockets.frames import CloseCode
from websockets.legacy.protocol import WebSocketCommonProtocol
from websockets.protocol import CONNECTING

from replit_river.common_session import (
    SessionState,
    TerminalStates,
    buffered_message_sender,
    check_to_close_session,
    setup_heartbeat,
)
from replit_river.error_schema import (
    ERROR_CODE_CANCEL,
    ERROR_CODE_STREAM_CLOSED,
    ERROR_HANDSHAKE,
    RiverError,
    RiverException,
    RiverServiceException,
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
    IgnoreMessageException,
    InvalidMessageException,
    OutOfOrderMessageException,
)
from replit_river.task_manager import BackgroundTaskManager
from replit_river.transport_options import (
    MAX_MESSAGE_BUFFER_SIZE,
    TransportOptions,
    UriAndMetadata,
)

STREAM_CANCEL_BIT_TYPE = Literal[0b00100]
STREAM_CANCEL_BIT: STREAM_CANCEL_BIT_TYPE = 0b00100
STREAM_CLOSED_BIT_TYPE = Literal[0b01000]
STREAM_CLOSED_BIT: STREAM_CLOSED_BIT_TYPE = 0b01000


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
    _connecting_task: asyncio.Task[Literal[True]] | None
    _connection_condition: asyncio.Condition

    # ws state
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
        transport_options: TransportOptions,
        close_session_callback: CloseSessionCallback,
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
        self._connection_condition = asyncio.Condition()

        # ws state
        self._ws_unwrapped = None
        self._heartbeat_misses = 0
        self._retry_connection_callback = retry_connection_callback

        # message state
        self._message_enqueued = asyncio.Semaphore()
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

        async def do_close_websocket() -> None:
            logger.debug(
                "do_close called, _state=%r, _ws_unwrapped=%r",
                self._state,
                self._ws_unwrapped,
            )
            if self._ws_unwrapped:
                self._task_manager.create_task(self._ws_unwrapped.close())
                if self._retry_connection_callback:
                    self._task_manager.create_task(self._retry_connection_callback())
                self._ws_unwrapped = None
            else:
                self._state = SessionState.CLOSING
            await self._begin_close_session_countdown()

        def increment_and_get_heartbeat_misses() -> int:
            self._heartbeat_misses += 1
            return self._heartbeat_misses

        self._task_manager.create_task(
            setup_heartbeat(
                self.session_id,
                self._transport_options.heartbeat_ms,
                self._transport_options.heartbeats_until_dead,
                lambda: self._state,
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

        def commit(msg: TransportMessage) -> None:
            pending = self._send_buffer.popleft()
            if msg.seq != pending.seq:
                logger.error("Out of sequence error")
            self._ack_buffer.append(pending)

            # On commit, release pending writers waiting for more buffer space
            if self._queue_full_lock.locked():
                self._queue_full_lock.release()

        def get_next_pending() -> TransportMessage | None:
            if self._send_buffer:
                return self._send_buffer[0]
            return None

        # TODO: Just return _ws_unwrapped once we are no longer using the legacy client
        def get_ws() -> WebSocketCommonProtocol | ClientConnection | None:
            if self.is_connected():
                return self._ws_unwrapped
            return None

        self._task_manager.create_task(
            buffered_message_sender(
                self._connection_condition,
                self._message_enqueued,
                get_ws=get_ws,
                websocket_closed_callback=self._begin_close_session_countdown,
                get_next_pending=get_next_pending,
                commit=commit,
            )
        )

    async def ensure_connected[HandshakeMetadata](
        self,
        client_id: str,
        rate_limiter: LeakyBucketRateLimit,
        uri_and_metadata_factory: Callable[
            [], Awaitable[UriAndMetadata[HandshakeMetadata]]
        ],  # noqa: E501
        protocol_version: str,
    ) -> None:
        """
        Either return immediately or establish a websocket connection and return
        once we can accept messages.

        One of the goals of this function is to gate exactly one call to the
        logic that actually establishes the connection.
        """

        logger.debug("ensure_connected: is_connected=%r", self.is_connected())
        if self.is_connected():
            return

        if not self._connecting_task:
            self._connecting_task = asyncio.create_task(
                self._do_ensure_connected(
                    client_id,
                    rate_limiter,
                    uri_and_metadata_factory,
                    protocol_version,
                )
            )

        await self._connecting_task

    async def _do_ensure_connected[HandshakeMetadata](
        self,
        client_id: str,
        rate_limiter: LeakyBucketRateLimit,
        uri_and_metadata_factory: Callable[
            [], Awaitable[UriAndMetadata[HandshakeMetadata]]
        ],  # noqa: E501
        protocol_version: str,
    ) -> Literal[True]:
        max_retry = self._transport_options.connection_retry_options.max_retry
        logger.info("Attempting to establish new ws connection")

        last_error: Exception | None = None
        i = 0
        await self._connection_condition.acquire()
        while rate_limiter.has_budget_or_throw(client_id, ERROR_HANDSHAKE, last_error):
            if i > 0:
                logger.info(f"Retrying build handshake number {i} times")
            i += 1

            rate_limiter.consume_budget(client_id)

            try:
                uri_and_metadata = await uri_and_metadata_factory()
                ws = await websockets.asyncio.client.connect(uri_and_metadata["uri"])

                try:
                    try:
                        expectedSessionState = ExpectedSessionState(
                            nextExpectedSeq=self.ack,
                            nextSentSeq=self.seq,
                        )
                        handshake_request = ControlMessageHandshakeRequest[
                            HandshakeMetadata
                        ](  # noqa: E501
                            type="HANDSHAKE_REQ",
                            protocolVersion=protocol_version,
                            sessionId=self.session_id,
                            metadata=uri_and_metadata["metadata"],
                            expectedSessionState=expectedSessionState,
                        )
                        stream_id = nanoid.generate()

                        async def websocket_closed_callback() -> None:
                            logger.error("websocket closed before handshake response")

                        await send_transport_message(
                            TransportMessage(
                                from_=self._transport_id,
                                to=self._to_id,
                                streamId=stream_id,
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
                    ) as e:  # noqa: E501
                        raise RiverException(
                            ERROR_HANDSHAKE,
                            "Handshake failed, conn closed while sending response",  # noqa: E501
                        ) from e

                    startup_grace_deadline_ms = await self._get_current_time() + 60_000
                    while True:
                        if await self._get_current_time() >= startup_grace_deadline_ms:  # noqa: E501
                            raise RiverException(
                                ERROR_HANDSHAKE,
                                "Handshake response timeout, closing connection",  # noqa: E501
                            )
                        try:
                            data = await ws.recv()
                        except ConnectionClosed as e:
                            logger.debug(
                                "Connection closed during waiting for handshake response",  # noqa: E501
                                exc_info=True,
                            )
                            raise RiverException(
                                ERROR_HANDSHAKE,
                                "Handshake failed, conn closed while waiting for response",  # noqa: E501
                            ) from e
                        try:
                            response_msg = parse_transport_msg(data)
                            break
                        except IgnoreMessageException:
                            logger.debug("Ignoring transport message", exc_info=True)  # noqa: E501
                            continue
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

                    logger.debug(
                        "river client get handshake response : %r", handshake_response
                    )  # noqa: E501
                    if not handshake_response.status.ok:
                        raise RiverException(
                            ERROR_HANDSHAKE,
                            f"Handshake failed with code {handshake_response.status.code}: "  # noqa: E501
                            + f"{handshake_response.status.reason}",
                        )

                    last_error = None
                    rate_limiter.start_restoring_budget(client_id)
                    self._state = SessionState.ACTIVE
                    self._ws_unwrapped = ws
                    self._connection_condition.notify_all()
                    break
                except RiverException as e:
                    await ws.close()
                    raise e
            except Exception as e:
                last_error = e
                backoff_time = rate_limiter.get_backoff_ms(client_id)
                logger.exception(
                    f"Error connecting, retrying with {backoff_time}ms backoff"
                )
                await asyncio.sleep(backoff_time / 1000)

        # We are in a state where we may throw an exception.
        #
        # To permit subsequent calls to ensure_connected to pass, we clear ourselves.
        # This is safe because each individual function that is waiting on this
        # function completeing already has a reference, so we'll last a few ticks
        # before GC.
        #
        # Let's do our best to avoid clobbering other tasks by comparing the .name
        current_task = asyncio.current_task()
        if (
            self._connecting_task
            and current_task
            and self._connecting_task.get_name() == current_task.get_name()
        ):
            self._connecting_task = None

        # Release the lock we took earlier so we can use it again in the next
        # connection attempt
        self._connection_condition.release()

        if last_error is not None:
            raise RiverException(
                ERROR_HANDSHAKE,
                f"Failed to create ws after retrying {max_retry} number of times",
            ) from last_error

        return True

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
        self._close_session_after_time_secs = close_session_after_time_secs

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
        if self._state in TerminalStates:
            return
        logger.debug(
            "send_message(stream_id=%r, payload=%r, control_flags=%r, "
            "service_name=%r, procedure_name=%r)",
            stream_id,
            payload,
            bin(control_flags),
            service_name,
            procedure_name,
        )
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
            logging.debug("send_message: queue full, waiting")
            await self._queue_full_lock.acquire()
        self._send_buffer.append(msg)
        # Wake up buffered_message_sender
        self._message_enqueued.release()
        self.seq += 1

    async def close(self) -> None:
        """Close the session and all associated streams."""
        logger.info(
            f"{self._transport_id} closing session "
            f"to {self._to_id}, ws: {self._ws_unwrapped}"
        )
        if self._state in TerminalStates:
            # already closing
            return
        self._state = SessionState.CLOSING
        self._reset_session_close_countdown()
        await self._task_manager.cancel_all_tasks()

        # TODO: unexpected_close should close stream differently here to
        # throw exception correctly.
        for stream in self._streams.values():
            stream.close()
        self._streams.clear()

        if self._ws_unwrapped:
            # The Session isn't guaranteed to live much longer than this close()
            # invocation, so let's await this close to avoid dropping the socket.
            await self._ws_unwrapped.close()

        self._state = SessionState.CLOSED

        # Clear the session in transports
        # This will get us GC'd, so this should be the last thing.
        await self._close_session_callback(self)

    async def start_serve_responses(self) -> None:
        async def transition_closed() -> None:
            self._state = SessionState.CONNECTING
            if self._retry_connection_callback:
                self._task_manager.create_task(self._retry_connection_callback())

            await self._begin_close_session_countdown()
        self._task_manager.create_task(self._serve(
            get_state=lambda: self._state,
            transition_closed=transition_closed,
            reset_session_close_countdown=self._reset_session_close_countdown,
        ))

    async def _serve(
        self,
        get_state: Callable[[], SessionState],
        transition_closed: Callable[[], Awaitable[None]],
        reset_session_close_countdown: Callable[[], None],
    ) -> None:
        """Serve messages from the websocket."""
        reset_session_close_countdown()
        our_task = asyncio.current_task()
        idx = 0
        while our_task and not our_task.cancelling() and not our_task.cancelled():
            logging.debug(f"_serve loop count={idx}")
            idx += 1
            try:
                try:
                    await self._handle_messages_from_ws()
                except ConnectionClosed:
                    # Set ourselves to closed as soon as we get the signal
                    await transition_closed()
                    logger.debug("ConnectionClosed while serving", exc_info=True)
                except FailedSendingMessageException:
                    # Expected error if the connection is closed.
                    logger.debug(
                        "FailedSendingMessageException while serving", exc_info=True
                    )
                except Exception:
                    logger.exception("caught exception at message iterator")
            except ExceptionGroup as eg:
                _, unhandled = eg.split(lambda e: isinstance(e, ConnectionClosed))
                if unhandled:
                    # We're in a task, there's not that much that can be done.
                    unhandled = ExceptionGroup(
                        "Unhandled exceptions on River server", unhandled.exceptions
                    )
                    logger.exception("caught exception at message iterator", exc_info=unhandled)
                    raise unhandled
        logging.debug(f"_serve exiting normally after {idx} loops")

    async def _handle_messages_from_ws(self) -> None:
        logging.debug("_handle_messages_from_ws started")
        while self._ws_unwrapped is None or self._state == SessionState.CONNECTING:
            logging.debug("_handle_messages_from_ws spinning while connecting")
            await asyncio.sleep(1)
        logger.debug(
            "%s start handling messages from ws %s",
            "client",
            self._ws_unwrapped.id,
        )
        try:
            # We should not process messages if the websocket is closed.
            while ws := self._ws_unwrapped:
                # decode=False: Avoiding an unnecessary round-trip through str
                # Ideally this should be type-ascripted to : bytes, but there is no
                # @overrides in `websockets` to hint this.
                message = await ws.recv(decode=False)
                try:
                    msg = parse_transport_msg(message)

                    logger.debug(f"{self._transport_id} got a message %r", msg)

                    # Update bookkeeping
                    if msg.seq < self.ack:
                        raise IgnoreMessageException(
                            f"{msg.from_} received duplicate msg, got {msg.seq}"
                            f" expected {self.ack}"
                        )
                    elif msg.seq > self.ack:
                        logger.warning(
                            f"Out of order message received got {msg.seq} expected "
                            f"{self.ack}"
                        )

                        raise OutOfOrderMessageException(
                            f"Out of order message received got {msg.seq} expected "
                            f"{self.ack}"
                        )

                    assert msg.seq == self.ack, "Safety net, redundant assertion"

                    # Set our next expected ack number
                    self.ack = msg.seq + 1

                    # Discard old server-ack'd messages from the ack buffer
                    while self._ack_buffer and self._ack_buffer[0].seq < msg.ack:
                        self._ack_buffer.popleft()

                    self._reset_session_close_countdown()

                    # Shortcut to avoid processing ack packets
                    if msg.controlFlags & ACK_BIT != 0:
                        continue

                    stream = self._streams.get(msg.streamId, None)
                    if msg.controlFlags & STREAM_OPEN_BIT != 0:
                        raise InvalidMessageException(
                            "Client should not receive stream open bit"
                        )

                    if not stream:
                        logger.warning("no stream for %s", msg.streamId)
                        raise IgnoreMessageException("no stream for message, ignoring")

                    if (
                        msg.controlFlags & STREAM_CLOSED_BIT != 0
                        and msg.payload.get("type", None) == "CLOSE"
                    ):
                        # close message is not sent to the stream
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
                        if stream:
                            stream.close()
                        del self._streams[msg.streamId]
                except IgnoreMessageException:
                    logger.debug("Ignoring transport message", exc_info=True)
                    continue
                except OutOfOrderMessageException:
                    logger.exception("Out of order message, closing connection")
                    self._task_manager.create_task(
                        self._ws_unwrapped.close(
                            code=CloseCode.INVALID_DATA,
                            reason="Out of order message",
                        )
                    )
                    return
                except InvalidMessageException:
                    logger.exception("Got invalid transport message, closing session")
                    await self.close()
                    return
        except ConnectionClosedOK:
            # Exited normally
            self._state = SessionState.CONNECTING
        except ConnectionClosed as e:
            self._state = SessionState.CONNECTING
            raise e
        logging.debug("_handle_messages_from_ws exiting")  # When the network disconnects this Task exits and then we don't restart it.

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
        output: Channel[Any] = Channel(1)
        self._streams[stream_id] = output
        await self.send_message(
            stream_id=stream_id,
            control_flags=STREAM_OPEN_BIT | STREAM_CLOSED_BIT,
            payload=request_serializer(request),
            service_name=service_name,
            procedure_name=procedure_name,
            span=span,
        )
        # Handle potential errors during communication
        try:
            try:
                async with asyncio.timeout(timeout.total_seconds()):
                    response = await output.get()
            except asyncio.TimeoutError as e:
                await self.send_message(
                    stream_id=stream_id,
                    control_flags=STREAM_CANCEL_BIT,
                    payload={"type": "CANCEL"},
                    service_name=service_name,
                    procedure_name=procedure_name,
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
            if not response.get("ok", False):
                try:
                    error = error_deserializer(response["payload"])
                except Exception as e:
                    raise RiverException("error_deserializer", str(e)) from e
                raise exception_from_message(error.code)(
                    error.code, error.message, service_name, procedure_name
                )
            return response_deserializer(response["payload"])
        except RiverException as e:
            raise e
        except Exception as e:
            raise e

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
        output: Channel[Any] = Channel(1)
        self._streams[stream_id] = output
        try:
            await self.send_message(
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
                    control_flags = 0
                    await self.send_message(
                        stream_id=stream_id,
                        service_name=service_name,
                        procedure_name=procedure_name,
                        control_flags=control_flags,
                        payload=request_serializer(item),
                        span=span,
                    )
        except Exception as e:
            raise RiverServiceException(
                ERROR_CODE_STREAM_CLOSED, str(e), service_name, procedure_name
            ) from e
        await self.send_close_stream(
            service_name,
            procedure_name,
            stream_id,
            extra_control_flags=0,
        )

        # Handle potential errors during communication
        # TODO: throw a error when the transport is hard closed
        try:
            try:
                response = await output.get()
            except ChannelClosed as e:
                raise RiverServiceException(
                    ERROR_CODE_STREAM_CLOSED,
                    "Stream closed before response",
                    service_name,
                    procedure_name,
                ) from e
            except RuntimeError as e:
                raise RiverException(ERROR_CODE_STREAM_CLOSED, str(e)) from e
            if not response.get("ok", False):
                try:
                    error = error_deserializer(response["payload"])
                except Exception as e:
                    raise RiverException("error_deserializer", str(e)) from e
                raise exception_from_message(error.code)(
                    error.code, error.message, service_name, procedure_name
                )

            return response_deserializer(response["payload"])
        except RiverException as e:
            raise e
        except Exception as e:
            raise e

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
        output: Channel[Any] = Channel(MAX_MESSAGE_BUFFER_SIZE)
        self._streams[stream_id] = output
        await self.send_message(
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
                if item.get("type", None) == "CLOSE":
                    break
                if not item.get("ok", False):
                    try:
                        yield error_deserializer(item["payload"])
                    except Exception:
                        logger.exception(
                            f"Error during subscription error deserialization: {item}"
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
        output: Channel[Any] = Channel(MAX_MESSAGE_BUFFER_SIZE)
        self._streams[stream_id] = output
        try:
            await self.send_message(
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
                await self.send_close_stream(
                    service_name,
                    procedure_name,
                    stream_id,
                    extra_control_flags=STREAM_OPEN_BIT,
                )
                return

            assert request_serializer, "send_stream missing request_serializer"

            async for item in request:
                if item is None:
                    continue
                await self.send_message(
                    service_name=service_name,
                    procedure_name=procedure_name,
                    stream_id=stream_id,
                    control_flags=0,
                    payload=request_serializer(item),
                )
            await self.send_close_stream(service_name, procedure_name, stream_id)

        self._task_manager.create_task(_encode_stream())

        # Handle potential errors during communication
        try:
            async for item in output:
                if "type" in item and item["type"] == "CLOSE":
                    break
                if not item.get("ok", False):
                    try:
                        yield error_deserializer(item["payload"])
                    except Exception:
                        logger.exception(
                            f"Error during subscription error deserialization: {item}"
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

    async def send_close_stream(
        self,
        service_name: str,
        procedure_name: str,
        stream_id: str,
        extra_control_flags: int = 0,
    ) -> None:
        # close stream
        await self.send_message(
            service_name=service_name,
            procedure_name=procedure_name,
            stream_id=stream_id,
            control_flags=STREAM_CLOSED_BIT | extra_control_flags,
            payload={
                "type": "CLOSE",
            },
        )
