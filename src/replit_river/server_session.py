import asyncio
import logging
from typing import Any, Callable, Coroutine, assert_never

import websockets
from aiochannel import Channel, ChannelClosed
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from websockets.exceptions import ConnectionClosed

from replit_river.messages import (
    FailedSendingMessageException,
    parse_transport_msg,
)
from replit_river.seq_manager import (
    IgnoreMessage,
    InvalidMessageException,
    OutOfOrderMessageException,
)
from replit_river.session import Session
from replit_river.transport_options import MAX_MESSAGE_BUFFER_SIZE, TransportOptions

from .rpc import (
    ACK_BIT,
    STREAM_OPEN_BIT,
    GenericRpcHandlerBuilder,
    TransportMessage,
    TransportMessageTracingSetter,
)

STREAM_CLOSED_BIT = 0x0004  # Synonymous with the cancel bit in v2


logger = logging.getLogger(__name__)


trace_propagator = TraceContextTextMapPropagator()
trace_setter = TransportMessageTracingSetter()


class ServerSession(Session):
    """A transport object that handles the websocket connection with a client."""

    handlers: dict[tuple[str, str], tuple[str, GenericRpcHandlerBuilder]]

    def __init__(
        self,
        transport_id: str,
        to_id: str,
        session_id: str,
        websocket: websockets.WebSocketCommonProtocol,
        transport_options: TransportOptions,
        handlers: dict[tuple[str, str], tuple[str, GenericRpcHandlerBuilder]],
        close_session_callback: Callable[[Session], Coroutine[Any, Any, Any]],
        retry_connection_callback: (
            Callable[
                [],
                Coroutine[Any, Any, Any],
            ]
            | None
        ) = None,
    ) -> None:
        super().__init__(
            transport_id=transport_id,
            to_id=to_id,
            session_id=session_id,
            websocket=websocket,
            transport_options=transport_options,
            close_session_callback=close_session_callback,
            retry_connection_callback=retry_connection_callback,
        )
        self._handlers = handlers

        async def do_close_websocket() -> None:
            await self.close_websocket(
                self._ws_wrapper,
                should_retry=False,
            )
            await self._begin_close_session_countdown()

        self._setup_heartbeats_task(do_close_websocket)

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

    async def _handle_messages_from_ws(self, tg: asyncio.TaskGroup) -> None:
        logger.debug(
            "%s start handling messages from ws %s",
            "server",
            self._ws_wrapper.id,
        )
        try:
            ws_wrapper = self._ws_wrapper
            async for message in ws_wrapper.ws:
                try:
                    if not ws_wrapper.is_open():
                        # We should not process messages if the websocket is closed.
                        break
                    msg = parse_transport_msg(message)
                    if isinstance(msg, str):
                        logger.debug("Ignoring transport message", exc_info=True)
                        continue

                    logger.debug(f"{self._transport_id} got a message %r", msg)

                    # Update bookkeeping
                    match self._seq_manager.check_seq_and_update(msg):
                        case IgnoreMessage():
                            continue
                        case None:
                            pass
                        case other:
                            assert_never(other)
                    await self._buffer.remove_old_messages(
                        self._seq_manager.receiver_ack,
                    )
                    self._reset_session_close_countdown()

                    if msg.controlFlags & ACK_BIT != 0:
                        continue
                    stream = self._streams.get(msg.streamId)
                    if msg.controlFlags & STREAM_OPEN_BIT == 0:
                        if not stream:
                            logger.warning("no stream for %s", msg.streamId)
                            continue

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
                    else:
                        _stream = await self._open_stream_and_call_handler(msg, tg)
                        if isinstance(_stream, IgnoreMessage):
                            continue
                        if not stream:
                            self._streams[msg.streamId] = _stream
                        stream = _stream

                    if msg.controlFlags & STREAM_CLOSED_BIT != 0:
                        if stream:
                            stream.close()
                        del self._streams[msg.streamId]
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
        tg: asyncio.TaskGroup,
    ) -> Channel | IgnoreMessage:
        if not msg.serviceName or not msg.procedureName:
            logger.warning(
                "Service name or procedure name is missing in the message %r",
                msg,
            )
            return IgnoreMessage()
        key = (msg.serviceName, msg.procedureName)
        handler = self._handlers.get(key, None)
        if not handler:
            logger.warning(
                "No handler for %r handlers: %r",
                key,
                self._handlers.keys(),
            )
            return IgnoreMessage()
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
