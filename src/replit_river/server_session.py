import asyncio
import logging
from typing import Any

from aiochannel import Channel, ChannelClosed
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from websockets.exceptions import ConnectionClosed

from replit_river.messages import (
    FailedSendingMessageException,
    parse_transport_msg,
)
from replit_river.seq_manager import (
    IgnoreMessageException,
    InvalidMessageException,
    OutOfOrderMessageException,
)
from replit_river.session import Session
from replit_river.transport_options import MAX_MESSAGE_BUFFER_SIZE

from .rpc import (
    ACK_BIT,
    STREAM_CLOSED_BIT,
    STREAM_OPEN_BIT,
    TransportMessage,
    TransportMessageTracingSetter,
)

logger = logging.getLogger(__name__)


logger = logging.getLogger(__name__)

trace_propagator = TraceContextTextMapPropagator()
trace_setter = TransportMessageTracingSetter()


class ServerSession(Session):
    """A transport object that handles the websocket connection with a client."""

    async def start_serve_responses(self) -> None:
        self._task_manager.create_task(self.serve())

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
        self, tg: asyncio.TaskGroup | None = None
    ) -> None:
        logger.debug(
            "%s start handling messages from ws %s",
            "server",
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

    async def _open_stream_and_call_handler(
        self,
        msg: TransportMessage,
        tg: asyncio.TaskGroup | None,
    ) -> Channel:
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
