import asyncio
import enum
import logging
from typing import Any, Callable, Coroutine, Dict, Optional, Tuple

import nanoid  # type: ignore
import websockets
from aiochannel import Channel, ChannelClosed
from websockets import WebSocketCommonProtocol
from websockets.exceptions import ConnectionClosedError

from replit_river.message_buffer import MessageBuffer
from replit_river.messages import (
    FailedSendingMessageException,
    parse_transport_msg,
    send_transport_message,
)
from replit_river.seq_manager import (
    IgnoreTransportMessageException,
    InvalidTransportMessageException,
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


class SessionState(enum.Enum):
    ACTIVE = 0
    CLOSING = 1
    CLOSED = 2


class Session(object):
    """A transport object that handles the websocket connection with a client."""

    def __init__(
        self,
        transport_id: str,
        to_id: str,
        instance_id: str,
        websocket: websockets.WebSocketCommonProtocol,
        transport_options: TransportOptions,
        close_session_callback: Callable[["Session"], Coroutine[Any, Any, None]],
        is_server: bool,
        handlers: Dict[Tuple[str, str], Tuple[str, GenericRpcHandler]],
        close_websocket_callback: Optional[
            Callable[["Session"], Coroutine[Any, Any, None]]
        ] = None,
    ) -> None:
        self._transport_id = transport_id
        self._to_id = to_id
        self._instance_id = instance_id
        self._handlers = handlers

        self._state = SessionState.ACTIVE

        self._ws_lock = asyncio.Lock()
        self._ws = websocket
        self._close_websocket_callback = close_websocket_callback

        self._close_session_callback = close_session_callback
        self._is_server = is_server
        self._streams: Dict[str, Channel[Any]] = {}
        self._transport_options = transport_options
        self._seq_manager = SeqManager()
        self._stream_lock = asyncio.Lock()
        self._task_manager = BackgroundTaskManager()
        self._buffer = MessageBuffer()
        self.heartbeat_misses = 0
        # should disconnect after this time
        self._close_session_after_time_secs: Optional[float] = None
        asyncio.create_task(self._task_manager.create_task(self._heartbeat(self._ws)))

    async def is_session_open(self) -> bool:
        return self._state == SessionState.ACTIVE

    async def is_websocket_open(self) -> bool:
        async with self._ws_lock:
            return self._ws.open

    async def serve(self) -> None:
        """Serve messages from the websocket."""
        try:
            async with asyncio.TaskGroup() as tg:
                try:
                    await self._handle_messages_from_ws(self._ws, tg)
                except ConnectionClosedError as e:
                    await self.begin_close_session_countdown()
                    logging.debug(f"ConnectionClosedError while serving: {e}")
                except FailedSendingMessageException as e:
                    # Expected error if the connection is closed.
                    logging.debug(f"FailedSendingMessageException while serving: {e}")
                except Exception:
                    logging.exception("caught exception at message iterator")
        except ExceptionGroup as eg:
            _, unhandled = eg.split(lambda e: isinstance(e, ConnectionClosedError))
            if unhandled:
                raise ExceptionGroup(
                    "Unhandled exceptions on River server", unhandled.exceptions
                )

    async def _handle_messages_from_ws(
        self, websocket: WebSocketCommonProtocol, tg: Optional[asyncio.TaskGroup] = None
    ) -> None:
        logging.debug(
            f'{"server" if self._is_server else "client"} start handling messages from'
            " ws"
        )
        try:
            async for message in websocket:
                try:
                    msg = parse_transport_msg(message, self._transport_options)

                    logging.debug("got a message %r", msg)

                    await self._seq_manager.check_seq_and_update(msg)
                    await self._update_msg_buffer()
                    # We get valid message, we should cancel the session close countdown
                    self.reset_session_close_countdown()
                    if msg.controlFlags & ACK_BIT != 0:
                        continue
                    async with self._stream_lock:
                        stream = self._streams.get(msg.streamId, None)
                    if msg.controlFlags & STREAM_OPEN_BIT == 0:
                        if not stream:
                            logging.warning("no stream for %s", msg.streamId)
                            raise IgnoreTransportMessageException(
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
                except IgnoreTransportMessageException as e:
                    logging.debug(f"Ignoring transport message : {e}")
                    continue
                except InvalidTransportMessageException as e:
                    logging.error(
                        f"Got invalid transport message, closing session : {e}"
                    )
                    await self.close()
                    return
        except ConnectionClosedError as e:
            raise e

    async def replace_with_new_websocket(
        self, websocket: websockets.WebSocketCommonProtocol
    ) -> None:
        logging.info("replacing with new websocket")
        await self.close_websocket(self._ws)
        self.reset_session_close_countdown()
        await self._send_buffered_messages(websocket)
        async with self._ws_lock:
            self._ws = websocket

    async def _get_current_time(self) -> float:
        return asyncio.get_event_loop().time()

    async def begin_close_session_countdown(self) -> None:
        if self._close_session_after_time_secs:
            return
        logging.debug(
            f"websocket closed from {self._transport_id} to {self._to_id}, "
            "begin grace period"
        )
        grace_period_ms = self._transport_options.session_disconnect_grace_ms
        self._close_session_after_time_secs = (
            await self._get_current_time() + grace_period_ms / 1000
        )

    def reset_session_close_countdown(self) -> None:
        self.heartbeat_misses = 0
        self._close_session_after_time_secs = None
        logging.info(f"Grace period cancelled for session to {self._transport_id}")

    async def _heartbeat(
        self,
        websocket: WebSocketCommonProtocol,
    ) -> None:
        logging.debug("Start heartbeat")
        while True:
            await asyncio.sleep(self._transport_options.heartbeat_ms / 1000)
            current_time = await self._get_current_time()
            if self._close_session_after_time_secs:
                if current_time > self._close_session_after_time_secs:
                    logging.info(
                        "Grace period ended for :"
                        f" {self._transport_id}, closing session"
                    )
                    await self.close()
                    return
                continue
            if self.heartbeat_misses >= self._transport_options.heartbeats_until_dead:
                logging.info("Heartbeat timed out for :" f" {self._transport_id}")
                await self.close_websocket(websocket)
                await self.begin_close_session_countdown()
                return
            try:
                await self.send_message(
                    str(nanoid.generate()),
                    websocket,
                    {
                        "ack": 0,
                    },
                    ACK_BIT,
                )
                self.heartbeat_misses += 1
            except FailedSendingMessageException:
                logging.error("heartbeat failed")
                return

    async def _send_buffered_messages(
        self, websocket: websockets.WebSocketCommonProtocol
    ) -> None:
        while not await self._buffer.empty():
            msg = await self._buffer.peek()
            if not msg:
                continue
            try:
                await self._send_transport_message(
                    msg,
                    websocket,
                )
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
                msg, ws, self.begin_close_session_countdown, prefix_bytes
            )
        except FailedSendingMessageException as e:
            raise e

    async def send_message(
        self,
        stream_id: str,
        ws: WebSocketCommonProtocol,
        payload: Dict,
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
            await self._buffer.put(msg)
        except Exception:
            # We should close the session when there are too many messages in buffer
            await self.close()
            return
        try:
            await self._send_transport_message(
                msg,
                ws,
                prefix_bytes=self._transport_options.get_prefix_bytes(),
            )
        except FailedSendingMessageException as e:
            raise e

    async def send_responses(
        self,
        stream_id: str,
        ws: WebSocketCommonProtocol,
        output: Channel[Any],
        is_streaming_output: bool,
    ) -> None:
        """Send serialized messages to the websockets."""
        logging.debug("sent response of stream %r", stream_id)
        try:
            async for payload in output:
                if not is_streaming_output:
                    await self.send_message(stream_id, ws, payload, STREAM_CLOSED_BIT)
                    return
                await self.send_message(stream_id, ws, payload)
            logging.debug("sent an end of stream %r", stream_id)
            await self.send_message(stream_id, ws, {"type": "CLOSE"}, STREAM_CLOSED_BIT)
        except FailedSendingMessageException as e:
            logging.error(f"Error while sending responses back : {e}")
        except (RuntimeError, ChannelClosed) as e:
            logging.error(f"Error while sending responses back : {e}")
        except Exception as e:
            logging.error(f"Unknown error while river sending responses back : {e}")

    async def close_websocket(self, websocket: WebSocketCommonProtocol) -> None:
        logging.info(
            f"River session from {self._transport_id} to {self._to_id} "
            "closing websocket"
        )
        async with self._ws_lock:
            if self._close_websocket_callback:
                await self._close_websocket_callback(self)
            if websocket:
                await websocket.close()

    async def _open_stream_and_call_handler(
        self,
        msg: TransportMessage,
        stream: Optional[Channel],
        tg: Optional[asyncio.TaskGroup],
    ) -> Channel:
        if not self._is_server:
            raise InvalidTransportMessageException(
                "Client should not receive stream open bit"
            )
        if not msg.serviceName or not msg.procedureName:
            raise IgnoreTransportMessageException(
                f"Service name or procedure name is missing in the message {msg}"
            )
        key = (msg.serviceName, msg.procedureName)
        handler = self._handlers.get(key, None)
        if not handler:
            raise IgnoreTransportMessageException(
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
            raise InvalidTransportMessageException(e)
        if not stream:
            async with self._stream_lock:
                self._streams[msg.streamId] = input_stream
        # Start the handler.
        await self._task_manager.create_task(
            handler_func(msg.from_, input_stream, output_stream), tg
        )
        await self._task_manager.create_task(
            self.send_responses(
                msg.streamId, self._ws, output_stream, is_streaming_output
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
            raise InvalidTransportMessageException(e)

    async def _update_msg_buffer(self) -> None:
        await self._buffer.remove_old_messages(self._seq_manager.receiver_ack)

    async def start_serve_messages(self) -> None:
        await self._task_manager.create_task(self.serve())

    async def close(self) -> None:
        """Close the session and all associated streams."""
        logging.info(f"Closing session from {self._transport_id} to {self._to_id}")
        if self._state == SessionState.CLOSING or self._state == SessionState.CLOSED:
            return
        self._state = SessionState.CLOSING
        for previous_input in self._streams.values():
            previous_input.close()
        async with self._stream_lock:
            self._streams.clear()
        await self._task_manager.cancel_all_tasks()
        await self.close_websocket(self._ws)
        # Clear the session in transports
        await self._close_session_callback(self)
        self._state = SessionState.CLOSED
