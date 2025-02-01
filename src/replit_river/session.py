import asyncio
import enum
import logging
from typing import Any, Callable, Coroutine, Dict, Optional, Tuple

import nanoid  # type: ignore
import websockets
from aiochannel import Channel, ChannelClosed
from opentelemetry.trace import Span, use_span
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from websockets.exceptions import ConnectionClosed

from replit_river.message_buffer import MessageBuffer, MessageBufferClosedError
from replit_river.messages import (
    FailedSendingMessageException,
    WebsocketClosedException,
    parse_transport_msg,
    send_transport_message,
)
from replit_river.seq_manager import (
    IgnoreMessageException,
    InvalidMessageException,
    OutOfOrderMessageException,
    SeqManager,
)
from replit_river.task_manager import BackgroundTaskManager
from replit_river.transport_options import MAX_MESSAGE_BUFFER_SIZE, TransportOptions
from replit_river.websocket_wrapper import WebsocketWrapper

from .rpc import (
    ACK_BIT,
    STREAM_CLOSED_BIT,
    STREAM_OPEN_BIT,
    GenericRpcHandler,
    TransportMessage,
    TransportMessageTracingSetter,
)

logger = logging.getLogger(__name__)

trace_propagator = TraceContextTextMapPropagator()
trace_setter = TransportMessageTracingSetter()


class SessionState(enum.Enum):
    """The state a session can be in.

    Can only transition from ACTIVE to CLOSING to CLOSED.
    """

    ACTIVE = 0
    CLOSING = 1
    CLOSED = 2


class Session(object):
    """A transport object that handles the websocket connection with a client."""

    def __init__(
        self,
        transport_id: str,
        to_id: str,
        session_id: str,
        websocket: websockets.WebSocketCommonProtocol,
        transport_options: TransportOptions,
        is_server: bool,
        handlers: Dict[Tuple[str, str], Tuple[str, GenericRpcHandler]],
        close_session_callback: Callable[["Session"], Coroutine[Any, Any, Any]],
        retry_connection_callback: Optional[
            Callable[
                [],
                Coroutine[Any, Any, Any],
            ]
        ] = None,
    ) -> None:
        self._transport_id = transport_id
        self._to_id = to_id
        self.session_id = session_id
        self._handlers = handlers
        self._is_server = is_server
        self._transport_options = transport_options

        # session state, only modified during closing
        self._state = SessionState.ACTIVE
        self._state_lock = asyncio.Lock()
        self._close_session_callback = close_session_callback
        self._close_session_after_time_secs: Optional[float] = None

        # ws state
        self._ws_lock = asyncio.Lock()
        self._ws_wrapper = WebsocketWrapper(websocket)
        self._heartbeat_misses = 0
        self._retry_connection_callback = retry_connection_callback

        # stream for tasks
        self._stream_lock = asyncio.Lock()
        self._streams: Dict[str, Channel[Any]] = {}

        # book keeping
        self._seq_manager = SeqManager()
        self._msg_lock = asyncio.Lock()
        self._buffer = MessageBuffer(self._transport_options.buffer_size)
        self._task_manager = BackgroundTaskManager()

        self._setup_heartbeats_task()

    def _setup_heartbeats_task(self) -> None:
        self._task_manager.create_task(self._heartbeat())
        self._task_manager.create_task(self._check_to_close_session())

    async def is_session_open(self) -> bool:
        async with self._state_lock:
            return self._state == SessionState.ACTIVE

    async def is_websocket_open(self) -> bool:
        async with self._ws_lock:
            return await self._ws_wrapper.is_open()

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

    async def serve(self) -> None:
        """Serve messages from the websocket."""
        self._reset_session_close_countdown()
        try:
            async with asyncio.TaskGroup() as tg:
                try:
                    await self._handle_messages_from_ws(tg)
                except ConnectionClosed:
                    if self._retry_connection_callback:
                        self._task_manager.create_task(
                            self._retry_connection_callback()
                        )

                    await self._begin_close_session_countdown()
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
                raise ExceptionGroup(
                    "Unhandled exceptions on River server", unhandled.exceptions
                )

    async def _update_book_keeping(self, msg: TransportMessage) -> None:
        await self._seq_manager.check_seq_and_update(msg)
        await self._remove_acked_messages_in_buffer()
        self._reset_session_close_countdown()

    async def _handle_messages_from_ws(
        self, tg: Optional[asyncio.TaskGroup] = None
    ) -> None:
        logger.debug(
            "%s start handling messages from ws %s",
            "server" if self._is_server else "client",
            self._ws_wrapper.id,
        )
        try:
            ws_wrapper = self._ws_wrapper
            async for message in ws_wrapper.ws:
                try:
                    if not await ws_wrapper.is_open():
                        # We should not process messages if the websocket is closed.
                        break
                    msg = parse_transport_msg(message, self._transport_options)

                    logger.debug(f"{self._transport_id} got a message %r", msg)

                    await self._update_book_keeping(msg)
                    if msg.controlFlags & ACK_BIT != 0:
                        continue
                    async with self._stream_lock:
                        stream = self._streams.get(msg.streamId, None)
                    if msg.controlFlags & STREAM_OPEN_BIT == 0:
                        if not stream:
                            logger.warning("no stream for %s", msg.streamId)
                            raise IgnoreMessageException(
                                "no stream for message, ignoring"
                            )
                        await self._add_msg_to_stream(msg, stream)
                    else:
                        # TODO(dstewart) This looks like it opens a new call to handler
                        #                on ever ws message, instead of demuxing and
                        #                routing.
                        _stream = await self._open_stream_and_call_handler(msg, tg)
                        if not stream:
                            async with self._stream_lock:
                                self._streams[msg.streamId] = _stream
                        stream = _stream

                    if msg.controlFlags & STREAM_CLOSED_BIT != 0:
                        if stream:
                            stream.close()
                        async with self._stream_lock:
                            del self._streams[msg.streamId]
                except IgnoreMessageException:
                    logger.debug("Ignoring transport message", exc_info=True)
                    continue
                except OutOfOrderMessageException:
                    logger.exception("Out of order message, closing connection")
                    await ws_wrapper.close()
                    return
                except InvalidMessageException:
                    logger.exception("Got invalid transport message, closing session")
                    await self.close()
                    return
        except ConnectionClosed as e:
            raise e

    async def replace_with_new_websocket(
        self, new_ws: websockets.WebSocketCommonProtocol
    ) -> None:
        async with self._ws_lock:
            old_wrapper = self._ws_wrapper
            old_ws_id = old_wrapper.ws.id
            if new_ws.id != old_ws_id:
                await old_wrapper.close()
            self._ws_wrapper = WebsocketWrapper(new_ws)
        await self._send_buffered_messages(new_ws)
        # Server will call serve itself.
        if not self._is_server:
            await self.start_serve_responses()

    async def _get_current_time(self) -> float:
        return asyncio.get_event_loop().time()

    def _reset_session_close_countdown(self) -> None:
        self._heartbeat_misses = 0
        self._close_session_after_time_secs = None

    async def _check_to_close_session(self) -> None:
        while True:
            await asyncio.sleep(
                self._transport_options.close_session_check_interval_ms / 1000
            )
            if self._state != SessionState.ACTIVE:
                # already closing
                return
            # calculate the value now before comparing it so that there are no
            # await points between the check and the comparison to avoid a TOCTOU
            # race.
            current_time = await self._get_current_time()
            if not self._close_session_after_time_secs:
                continue
            if current_time > self._close_session_after_time_secs:
                logger.info(
                    "Grace period ended for %s, closing session", self._transport_id
                )
                await self.close()
                return

    async def _heartbeat(
        self,
    ) -> None:
        logger.debug("Start heartbeat")
        while True:
            await asyncio.sleep(self._transport_options.heartbeat_ms / 1000)
            if self._state != SessionState.ACTIVE:
                logger.debug(
                    "Session is closed, no need to send heartbeat, state : "
                    "%r close_session_after_this: %r",
                    {self._state},
                    {self._close_session_after_time_secs},
                )
                # session is closing / closed, no need to send heartbeat anymore
                return
            try:
                await self.send_message(
                    "heartbeat",
                    # TODO: make this a message class
                    # https://github.com/replit/river/blob/741b1ea6d7600937ad53564e9cf8cd27a92ec36a/transport/message.ts#L42
                    {
                        "ack": 0,
                    },
                    ACK_BIT,
                )
                self._heartbeat_misses += 1
                if (
                    self._heartbeat_misses
                    > self._transport_options.heartbeats_until_dead
                ):
                    if self._close_session_after_time_secs is not None:
                        # already in grace period, no need to set again
                        continue
                    logger.info(
                        "%r closing websocket because of heartbeat misses",
                        self.session_id,
                    )
                    await self.close_websocket(
                        self._ws_wrapper, should_retry=not self._is_server
                    )
                    await self._begin_close_session_countdown()
                    continue
            except FailedSendingMessageException:
                # this is expected during websocket closed period
                continue

    async def _send_buffered_messages(
        self, websocket: websockets.WebSocketCommonProtocol
    ) -> None:
        buffered_messages = list(self._buffer.buffer)
        for msg in buffered_messages:
            try:
                await self._send_transport_message(
                    msg,
                    websocket,
                )
            except WebsocketClosedException:
                logger.info(
                    "Connection closed while sending buffered messages", exc_info=True
                )
                break
            except FailedSendingMessageException:
                logger.exception("Error while sending buffered messages")
                break

    async def _send_transport_message(
        self,
        msg: TransportMessage,
        websocket: websockets.WebSocketCommonProtocol,
    ) -> None:
        try:
            await send_transport_message(
                msg, websocket, self._begin_close_session_countdown
            )
        except WebsocketClosedException as e:
            raise e
        except FailedSendingMessageException as e:
            raise e

    async def get_next_expected_seq(self) -> int:
        """Get the next expected sequence number from the server."""
        return await self._seq_manager.get_ack()

    async def get_next_sent_seq(self) -> int:
        """Get the next sequence number that the client will send."""
        nextMessage = await self._buffer.peek()
        if nextMessage:
            return nextMessage.seq
        return await self._seq_manager.get_seq()

    async def get_next_expected_ack(self) -> int:
        """Get the next expected ack that the client expects."""
        return await self._seq_manager.get_seq()

    async def send_message(
        self,
        stream_id: str,
        payload: Dict | str,
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
            from_=self._transport_id,  # type: ignore
            to=self._to_id,
            seq=await self._seq_manager.get_seq_and_increment(),
            ack=await self._seq_manager.get_ack(),
            controlFlags=control_flags,
            payload=payload,
            serviceName=service_name,
            procedureName=procedure_name,
        )
        if span:
            with use_span(span):
                trace_propagator.inject(msg, None, trace_setter)
        try:
            # We need this lock to ensure the buffer order and message sending order
            # are the same.
            async with self._msg_lock:
                try:
                    await self._buffer.put(msg)
                except MessageBufferClosedError:
                    # The session is closed and is no longer accepting new messages.
                    return
                async with self._ws_lock:
                    if not await self._ws_wrapper.is_open():
                        # If the websocket is closed, we should not send the message
                        # and wait for the retry from the buffer.
                        return
                await self._send_transport_message(
                    msg,
                    self._ws_wrapper.ws,
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

    async def _send_responses_from_output_stream(
        self,
        stream_id: str,
        output: Channel[Any],
        is_streaming_output: bool,
    ) -> None:
        """Send serialized messages to the websockets."""
        try:
            async for payload in output:
                if not is_streaming_output:
                    await self.send_message(stream_id, payload, STREAM_CLOSED_BIT)
                    return
                await self.send_message(stream_id, payload)
            logger.debug("sent an end of stream %r", stream_id)
            await self.send_message(stream_id, {"type": "CLOSE"}, STREAM_CLOSED_BIT)
        except FailedSendingMessageException:
            logger.exception("Error while sending responses")
        except (RuntimeError, ChannelClosed):
            logger.exception("Error while sending responses")
        except Exception:
            logger.exception("Unknown error while river sending responses back")

    async def close_websocket(
        self, ws_wrapper: WebsocketWrapper, should_retry: bool
    ) -> None:
        """Mark the websocket as closed, close the websocket, and retry if needed."""
        async with self._ws_lock:
            # Already closed.
            if not await ws_wrapper.is_open():
                return
            await ws_wrapper.close()
        if should_retry and self._retry_connection_callback:
            self._task_manager.create_task(self._retry_connection_callback())

    async def _open_stream_and_call_handler(
        self,
        msg: TransportMessage,
        tg: Optional[asyncio.TaskGroup],
    ) -> Channel:
        if not self._is_server:
            raise InvalidMessageException("Client should not receive stream open bit")
        if not msg.serviceName or not msg.procedureName:
            raise IgnoreMessageException(
                f"Service name or procedure name is missing in the message {msg}"
            )
        key = (msg.serviceName, msg.procedureName)
        handler = self._handlers.get(key, None)
        if not handler:
            raise IgnoreMessageException(
                f"No handler for {key} handlers : {self._handlers.keys()}"
            )
        method_type, handler_func = handler
        is_streaming_output = method_type in (
            "subscription-stream",  # subscription
            "stream",
        )
        is_streaming_input = method_type in (
            "upload-stream",  # subscription
            "stream",
        )
        # New channel pair.
        input_stream: Channel[Any] = Channel(
            MAX_MESSAGE_BUFFER_SIZE if is_streaming_input else 1
        )
        output_stream: Channel[Any] = Channel(
            MAX_MESSAGE_BUFFER_SIZE if is_streaming_output else 1
        )
        if (
            msg.controlFlags & STREAM_CLOSED_BIT == 0
            or msg.payload.get("type", None) != "CLOSE"
        ):
            try:
                await input_stream.put(msg.payload)
            except (RuntimeError, ChannelClosed) as e:
                raise InvalidMessageException(e) from e
        # Start the handler.
        self._task_manager.create_task(
            handler_func(msg.from_, input_stream, output_stream), tg
        )
        self._task_manager.create_task(
            self._send_responses_from_output_stream(
                msg.streamId, output_stream, is_streaming_output
            ),
            tg,
        )
        return input_stream

    async def _add_msg_to_stream(
        self,
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

    async def _remove_acked_messages_in_buffer(self) -> None:
        await self._buffer.remove_old_messages(self._seq_manager.receiver_ack)

    async def start_serve_responses(self) -> None:
        self._task_manager.create_task(self.serve())

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
            async with self._stream_lock:
                self._streams.clear()

            self._state = SessionState.CLOSED
