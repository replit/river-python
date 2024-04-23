import asyncio
import enum
import logging
from typing import Any, Callable, Coroutine, Dict, Optional, Tuple
import nanoid  # type: ignore
import websockets
from aiochannel import Channel, ChannelClosed
from websockets import WebSocketCommonProtocol
from websockets.exceptions import ConnectionClosed

from replit_river.message_buffer import MessageBuffer
from replit_river.messages import (
    FailedSendingMessageException,
    parse_transport_msg,
    send_transport_message,
)
from replit_river.seq_manager import (
    IgnoreMessageException,
    InvalidMessageException,
    SeqManager,
)
from replit_river.task_manager import BackgroundTaskManager
from replit_river.transport_options import TransportOptions

from .rpc import (
    ACK_BIT,
    STREAM_CLOSED_BIT,
    STREAM_OPEN_BIT,
    GenericRpcHandler,
    TransportMessage,
)
from replit_river.rpc import (
    ControlMessageHandshakeRequest,
)


class SessionState(enum.Enum):
    ACTIVE = 0
    CLOSING = 3
    CLOSED = 4


class WsState(enum.Enum):
    OPEN = 0
    CLOSING = 2
    CLOSED = 3


class Session(object):
    """A transport object that handles the websocket connection with a client."""

    def __init__(
        self,
        transport_id: str,
        to_id: str,
        instance_id: str,
        websocket: websockets.WebSocketCommonProtocol,
        transport_options: TransportOptions,
        is_server: bool,
        handlers: Dict[Tuple[str, str], Tuple[str, GenericRpcHandler]],
        close_session_callback: Callable[["Session"], Coroutine[Any, Any, None]],
        retry_connection_callback: Optional[
            Callable[
                ["Session"],
                Coroutine[Any, Any, None],
            ]
        ] = None,
    ) -> None:
        self._transport_id = transport_id
        self._to_id = to_id
        self._instance_id = instance_id
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
        self._ws_state = WsState.OPEN
        self._ws = websocket
        self._heartbeat_misses = 0
        self._retry_connection_callback = retry_connection_callback

        # stream for tasks
        self._stream_lock = asyncio.Lock()
        self._streams: Dict[str, Channel[Any]] = {}

        # book keeping
        self._seq_manager = SeqManager()
        self._buffer = MessageBuffer(self._transport_options.buffer_size)
        self._task_manager = BackgroundTaskManager()

        asyncio.create_task(self._setup_heartbeats_task())

    async def _setup_heartbeats_task(self) -> None:
        await self._task_manager.create_task(self._heartbeat())
        await self._task_manager.create_task(self._check_to_close_session())

    async def is_session_open(self) -> bool:
        async with self._state_lock:
            return self._state == SessionState.ACTIVE

    async def is_websocket_open(self) -> bool:
        async with self._ws_lock:
            return self._ws_state == WsState.OPEN

    async def _on_websocket_unexpected_close(self) -> None:
        """Handle unexpected websocket close."""
        logging.info(
            f"Unexpected websocket close from {self._transport_id} to {self._to_id}"
        )
        await self._begin_close_session_countdown()

    async def _begin_close_session_countdown(self) -> None:
        """Begin the countdown to close session, this should be called when
        websocket is closed.
        """
        logging.debug("begin_close_session_countdown")
        if self._close_session_after_time_secs is not None:
            # already in grace period, no need to set again
            return
        logging.debug(
            f"websocket closed from {self._transport_id} to {self._to_id}, "
            "begin grace period"
        )
        grace_period_ms = self._transport_options.session_disconnect_grace_ms
        self._close_session_after_time_secs = (
            await self._get_current_time() + grace_period_ms / 1000
        )

    async def serve(self) -> None:
        """Serve messages from the websocket."""
        try:
            async with asyncio.TaskGroup() as tg:
                try:
                    await self._handle_messages_from_ws(self._ws, tg)
                except ConnectionClosed as e:
                    await self._on_websocket_unexpected_close()
                    logging.debug(f"ConnectionClosed while serving: {e}")
                except FailedSendingMessageException as e:
                    # Expected error if the connection is closed.
                    logging.debug(f"FailedSendingMessageException while serving: {e}")
                except Exception:
                    logging.exception("caught exception at message iterator")
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
        self, websocket: WebSocketCommonProtocol, tg: Optional[asyncio.TaskGroup] = None
    ) -> None:
        logging.debug(
            f'{"server" if self._is_server else "client"} start handling messages from'
            f" ws {websocket.id}, state {websocket.state}"
        )
        try:
            async for message in websocket:
                try:
                    msg = parse_transport_msg(message, self._transport_options)

                    logging.debug(f"{self._transport_id} got a message %r", msg)

                    await self._update_book_keeping(msg)
                    if msg.controlFlags & ACK_BIT != 0:
                        continue
                    async with self._stream_lock:
                        stream = self._streams.get(msg.streamId, None)
                    if msg.controlFlags & STREAM_OPEN_BIT == 0:
                        if not stream:
                            logging.warning("no stream for %s", msg.streamId)
                            raise IgnoreMessageException(
                                "no stream for message, ignoring"
                            )
                        await self._add_msg_to_stream(msg, stream)
                    else:
                        stream = await self._open_stream_and_call_handler(
                            msg, stream, tg
                        )

                    if msg.controlFlags & STREAM_CLOSED_BIT != 0:
                        if stream:
                            stream.close()
                        async with self._stream_lock:
                            del self._streams[msg.streamId]
                except IgnoreMessageException as e:
                    logging.debug(f"Ignoring transport message : {e}")
                    continue
                except InvalidMessageException as e:
                    logging.error(
                        f"Got invalid transport message, closing session : {e}"
                    )
                    await self.close(True)
                    return
        except ConnectionClosed as e:
            raise e

    async def replace_with_new_websocket(
        self, new_ws: websockets.WebSocketCommonProtocol
    ) -> None:
        async with self._ws_lock:
            old_ws = self._ws
            self._ws_state = WsState.CLOSING
        if new_ws.id != old_ws.id:
            self._reset_session_close_countdown()
            await self.close_websocket(old_ws, should_retry=False)
        async with self._ws_lock:
            self._ws = new_ws
            self._ws_state = WsState.OPEN
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
            if not self._close_session_after_time_secs:
                continue
            current_time = await self._get_current_time()
            if current_time > self._close_session_after_time_secs:
                logging.info(
                    "Grace period ended for :" f" {self._transport_id}, closing session"
                )
                await self.close(False)
                return

    async def _heartbeat(
        self,
    ) -> None:
        logging.debug("Start heartbeat")
        while True:
            await asyncio.sleep(self._transport_options.heartbeat_ms / 1000)
            if (
                self._state != SessionState.ACTIVE
                or self._close_session_after_time_secs
            ):
                logging.debug(
                    f"Session is closed, no need to send heartbeat, state : "
                    f"{self._state} close_session_after_this: "
                    f"{self._close_session_after_time_secs}"
                )
                # session is closing, no need to send heartbeat
                continue
            try:
                await self.send_message(
                    str(nanoid.generate()),
                    {
                        "ack": 0,
                    },
                    self._ws,
                    ACK_BIT,
                )
                self._heartbeat_misses += 1
                if (
                    self._heartbeat_misses
                    >= self._transport_options.heartbeats_until_dead
                ):
                    logging.debug("closing websocket because of heartbeat misses")
                    await self._on_websocket_unexpected_close()
                    await self.close_websocket(
                        self._ws, should_retry=not self._is_server
                    )
                    continue
            except FailedSendingMessageException:
                # this is expected during websocket closed period
                continue

    async def _send_buffered_messages(
        self, websocket: websockets.WebSocketCommonProtocol
    ) -> None:
        logging.debug(f"Sending buffered messages to {self._to_id}")
        buffered_messages = list(self._buffer.buffer)
        for msg in buffered_messages:
            try:
                await self._send_transport_message(
                    msg,
                    websocket,
                    prefix_bytes=self._transport_options.get_prefix_bytes(),
                )
            except ConnectionClosed as e:
                logging.info(f"Connection closed while sending buffered messages : {e}")
                break
            except FailedSendingMessageException as e:
                logging.error(f"Error while sending buffered messages : {e}")
                break

    async def _send_transport_message(
        self,
        msg: TransportMessage,
        ws: WebSocketCommonProtocol,
        prefix_bytes: bytes = b"",
    ) -> None:
        try:
            await send_transport_message(
                msg, ws, self._on_websocket_unexpected_close, prefix_bytes
            )
        except ConnectionClosed as e:
            raise e
        except FailedSendingMessageException as e:
            raise e

    async def send_message(
        self,
        stream_id: str,
        payload: Dict | str,
        ws: WebSocketCommonProtocol,
        control_flags: int = 0,
        service_name: str | None = None,
        procedure_name: str | None = None,
    ) -> None:
        """Send serialized messages to the websockets."""
        msg = TransportMessage(
            streamId=stream_id,
            id=nanoid.generate(),
            from_=self._transport_id,
            to=self._to_id,
            seq=await self._seq_manager.get_seq_and_increment(),
            ack=await self._seq_manager.get_ack(),
            controlFlags=control_flags,
            payload=payload,
            serviceName=service_name,
            procedureName=procedure_name,
        )
        try:
            await self._send_transport_message(
                msg,
                ws,
                prefix_bytes=self._transport_options.get_prefix_bytes(),
            )
        except ConnectionClosed as e:
            logging.error(
                f"Connection closed while sending message : {e}, waiting for "
                "retry from buffer"
            )
        except FailedSendingMessageException as e:
            logging.error(
                f"Failed sending message : {e}, waiting for retry from buffer"
            )
        finally:
            try:
                await self._buffer.put(msg)
            except Exception:
                # We should close the session when there are too many messages in buffer
                await self.close(True)
                return

    async def _send_responses_from_output_stream(
        self,
        stream_id: str,
        output: Channel[Any],
        is_streaming_output: bool,
    ) -> None:
        """Send serialized messages to the websockets."""
        try:
            async for payload in output:
                # TODO: what if the websocket changed during this?
                ws = self._ws
                while self._ws_state != WsState.OPEN:
                    await asyncio.sleep(
                        self._transport_options.close_session_check_interval_ms / 1000
                    )
                    ws = self._ws
                if not is_streaming_output:
                    await self.send_message(stream_id, payload, ws, STREAM_CLOSED_BIT)
                    return
                await self.send_message(stream_id, payload, ws)
            logging.debug("sent an end of stream %r", stream_id)
            await self.send_message(stream_id, {"type": "CLOSE"}, ws, STREAM_CLOSED_BIT)
        except FailedSendingMessageException as e:
            logging.error(
                f"Error while sending responses, ws_state: {ws.state}, {type(e)} : {e}"
            )
        except (RuntimeError, ChannelClosed) as e:
            logging.error(
                f"Error while sending responses, ws_state: {ws.state} {type(e)} : {e}"
            )
        except Exception as e:
            logging.error(f"Unknown error while river sending responses back : {e}")

    async def close_websocket(
        self, ws: WebSocketCommonProtocol, should_retry: bool
    ) -> None:
        """Mark the websocket as closed, close the websocket, and retry if needed."""
        async with self._ws_lock:
            if self._ws.id != ws.id:
                # already replaced with new ws
                return
            if self._ws_state != WsState.OPEN:
                # Already closed
                return
            logging.info(
                f"River session from {self._transport_id} to {self._to_id} "
                f"closing websocket {ws.id} state: {ws.state}"
            )
            self._ws_state = WsState.CLOSING
            if ws:
                # TODO: should we wait here?
                task = asyncio.create_task(ws.close())
                task.add_done_callback(
                    lambda _: logging.debug(f"old websocket {ws.id} closed.")
                )
            self._ws_state = WsState.CLOSED
        if should_retry and self._retry_connection_callback:
            await self._retry_connection_callback(self)

    async def _open_stream_and_call_handler(
        self,
        msg: TransportMessage,
        stream: Optional[Channel],
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
                f"No handler for {key} handlers : " f"{self._handlers.keys()}"
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
        input_stream: Channel[Any] = Channel(1024 if is_streaming_input else 1)
        output_stream: Channel[Any] = Channel(1024 if is_streaming_output else 1)
        try:
            await input_stream.put(msg.payload)
        except (RuntimeError, ChannelClosed) as e:
            raise InvalidMessageException(e)
        if not stream:
            async with self._stream_lock:
                self._streams[msg.streamId] = input_stream
        # Start the handler.
        await self._task_manager.create_task(
            handler_func(msg.from_, input_stream, output_stream), tg
        )
        await self._task_manager.create_task(
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
        except (RuntimeError, ChannelClosed) as e:
            raise InvalidMessageException(e)

    async def _remove_acked_messages_in_buffer(self) -> None:
        await self._buffer.remove_old_messages(self._seq_manager.receiver_ack)

    async def start_serve_responses(self) -> None:
        await self._task_manager.create_task(self.serve())

    async def close(self, is_unexpected_close: bool) -> None:
        """Close the session and all associated streams."""
        logging.info(
            f"{self._transport_id} closing session "
            f"to {self._to_id} current_state : {self._ws_state}"
        )
        async with self._state_lock:
            if self._state != SessionState.ACTIVE:
                # already closing
                return
            self._state = SessionState.CLOSING
            self._reset_session_close_countdown()
            await self.close_websocket(self._ws, should_retry=False)
            # Clear the session in transports
            await self._close_session_callback(self)
            await self._task_manager.cancel_all_tasks()
            # TODO: unexpected_close should close stream differently here to
            # throw exception correctly.
            for stream in self._streams.values():
                stream.close()
            async with self._stream_lock:
                self._streams.clear()
            self._state = SessionState.CLOSED
