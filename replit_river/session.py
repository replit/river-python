import asyncio
import logging
from typing import Any, Callable, Coroutine, Dict, Optional, Tuple

import nanoid  # type: ignore
import websockets
from aiochannel import Channel
from websockets import WebSocketCommonProtocol
from websockets.exceptions import ConnectionClosedError
from websockets.server import WebSocketServerProtocol

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
from replit_river.transport import TransportOptions

from .rpc import (
    ACK_BIT,
    STREAM_CLOSED_BIT,
    STREAM_OPEN_BIT,
    GenericRpcHandler,
    TransportMessage,
)


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
    ) -> None:
        self._transport_id = transport_id
        self._to_id = to_id
        self._instance_id = instance_id
        self._handlers = handlers
        self._websocket = websocket
        self._close_session_callback = close_session_callback
        self._is_server = is_server
        self._streams: Dict[str, Channel[Any]] = {}
        self._transport_options = transport_options
        self._seq_manager = SeqManager()
        self._task_manager = BackgroundTaskManager()
        self._buffer: asyncio.Queue[TransportMessage] = asyncio.Queue(1000)
        self._lock = asyncio.Lock()
        self.heartbeat_misses = 0
        # should disconnect after this time
        self._should_disconnect_time: Optional[float] = None

    async def replace_with_new_websocket(
        self, websocket: websockets.WebSocketCommonProtocol
    ) -> None:
        if not self._websocket.closed:
            await self.close_stale_connection(self._websocket)
        self.cancel_grace()
        await self._send_buffered_messages(websocket)
        self._websocket = websocket

    async def _get_current_time(self) -> float:
        return asyncio.get_event_loop().time()

    async def begin_grace(self) -> None:
        grace_period_ms = self._transport_options.session_disconnect_grace_ms
        self._should_disconnect_time = (
            await self._get_current_time() + grace_period_ms / 1000
        )

    def cancel_grace(self) -> None:
        self.heartbeat_misses = 0
        self._should_disconnect_time = None
        logging.info(f"Grace period cancelled for session to {self._transport_id}")

    async def _heartbeat(
        self,
        msg: TransportMessage,
        websocket: WebSocketServerProtocol,
    ) -> None:
        logging.debug("Start heartbeat")
        while True:
            await asyncio.sleep(self._transport_options.heartbeat_ms / 1000)
            current_time = await self._get_current_time()
            if (
                self._should_disconnect_time
                and current_time > self._should_disconnect_time
            ):
                logging.info(
                    "Grace period ended for :"
                    f" {self._transport_id}, closing websocket"
                )
                await self.close()
                return
            if self.heartbeat_misses >= self._transport_options.heartbeats_until_dead:
                logging.info("Heartbeat timed out for :" f" {self._transport_id}")
                await self.close_stale_connection(websocket)
                await self.begin_grace()
                return
            try:
                await self.send_message(
                    msg.streamId,
                    websocket,
                    {
                        "ack": msg.id,
                    },
                    ACK_BIT,
                )
                self.heartbeat_misses += 1
            except ConnectionClosedError:
                logging.debug("heartbeat failed")
                return

    async def _send_buffered_messages(
        self, websocket: websockets.WebSocketCommonProtocol
    ) -> None:
        while not self._buffer.empty():
            msg = await self._buffer.get()
            try:
                await send_transport_message(msg, websocket)
            except FailedSendingMessageException as e:
                # Put the message back, they need to be resent
                async with self._lock:
                    msg_not_sent = [msg]
                    while not self._buffer.empty():
                        msg_not_sent.append(await self._buffer.get())
                    for msg in msg_not_sent:
                        self._buffer.put_nowait(msg)
                raise FailedSendingMessageException(
                    f"Failed to resend message during reconnecting : {e}"
                )

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
            service_name=service_name,
            procedure_name=procedure_name,
        )
        self._buffer.put_nowait(msg)
        await send_transport_message(
            msg,
            ws,
            prefix_bytes=self._transport_options.get_prefix_bytes(),
        )

    async def send_responses(
        self,
        stream_id: str,
        ws: WebSocketCommonProtocol,
        output: Channel[Any],
        is_stream: bool,
    ) -> None:
        """Send serialized messages to the websockets."""
        logging.debug("sent response of stream %r", stream_id)
        async for payload in output:
            if not is_stream:
                await self.send_message(stream_id, ws, payload, STREAM_CLOSED_BIT)
                return
            await self.send_message(stream_id, ws, payload)
        logging.debug("sent an end of stream %r", stream_id)
        await self.send_message(stream_id, ws, {"type": "CLOSE"}, STREAM_CLOSED_BIT)

    async def close_stale_connection(self, websocket: WebSocketCommonProtocol) -> None:
        await websocket.close()

    async def _handle_msg_server(
        self,
        msg: TransportMessage,
        stream: Optional[Channel],
        tg: Optional[asyncio.TaskGroup],
    ) -> None:
        if msg.controlFlags & STREAM_OPEN_BIT != 0:
            if not msg.serviceName or not msg.procedureName:
                logging.warning("no service or procedure name in %r", msg)
                return
            key = (msg.serviceName, msg.procedureName)
            handler = self._handlers.get(key, None)
            if not handler:
                logging.exception(
                    "No handler for %s handlers : " f"{self._handlers.keys()}",
                    key,
                )
                return
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
            await input_stream.put(msg.payload)
            if not stream:
                # We'll need to save it for later.
                self._streams[msg.streamId] = input_stream
            # Start the handler.
            await self._task_manager.create_task(
                handler_func(msg.from_, input_stream, output_stream), tg
            )
            await self._task_manager.create_task(
                self.send_responses(
                    msg.streamId, self._websocket, output_stream, is_streaming_output
                ),
                tg,
            )

        else:
            await self._add_msg_to_stream(msg, stream)

    async def _add_msg_to_stream(
        self,
        msg: TransportMessage,
        stream: Optional[Channel],
    ) -> None:
        if not stream:
            logging.warning("no stream for %s", msg.streamId)
            raise IgnoreTransportMessageException("no stream for message, ignoring")
        if not (
            msg.controlFlags & STREAM_CLOSED_BIT != 0
            and msg.payload.get("type", None) == "CLOSE"
        ):
            # close message is not sent to the stream
            await stream.put(msg.payload)

    async def handle_messages_from_ws(
        self, websocket: WebSocketCommonProtocol, tg: Optional[asyncio.TaskGroup] = None
    ) -> None:
        async for message in websocket:
            try:
                msg = parse_transport_msg(message, self._transport_options)
            except IgnoreTransportMessageException as e:
                logging.debug(f"Ignoring transport message : {e}")
                continue
            except InvalidTransportMessageException:
                logging.error("Got invalid transport message, closing connection")
                await self.close()
                return

            logging.debug("got a message %r", msg)

            try:
                await self._seq_manager.check_seq_and_update(msg)
            except IgnoreTransportMessageException:
                continue
            except InvalidTransportMessageException:
                return
            if msg.controlFlags & ACK_BIT != 0:
                self.heartbeat_misses = 0
                self.cancel_grace()
                continue

            stream = self._streams.get(msg.streamId, None)
            if self._is_server:
                await self._handle_msg_server(msg, stream, tg)
            else:
                await self._add_msg_to_stream(msg, stream)
            if msg.controlFlags & STREAM_CLOSED_BIT != 0:
                if stream:
                    stream.close()
                del self._streams[msg.streamId]

    async def start_serve_messages(self) -> None:
        await self._task_manager.create_task(self._serve())

    async def _serve(self) -> None:
        try:
            async with asyncio.TaskGroup() as tg:
                try:
                    await self.handle_messages_from_ws(self._websocket, tg)
                except ConnectionClosedError as e:
                    # This is fine.
                    logging.debug(f"ConnectionClosedError while serving: {e}")
                    pass
                except FailedSendingMessageException as e:
                    # Expected error if the connection is closed.
                    logging.debug(f"FailedSendingMessageException while serving: {e}")
                    pass
                except Exception:
                    logging.exception("caught exception at message iterator")
                finally:
                    await self.close()
        except ExceptionGroup as eg:
            _, unhandled = eg.split(lambda e: isinstance(e, ConnectionClosedError))
            if unhandled:
                raise ExceptionGroup(
                    "Unhandled exceptions on River server", unhandled.exceptions
                )

    async def close_websocket(self) -> None:
        """Close the websocket connection."""
        await self._websocket.close()

    async def close(self) -> None:
        """Close the session and all associated streams."""
        for previous_input in self._streams.values():
            previous_input.close()
        self._streams.clear()
        await self._task_manager.cancel_all_tasks()
        if self._websocket:
            await self._websocket.close()
        await self._close_session_callback(self)
