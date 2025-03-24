import asyncio
import logging
from collections.abc import AsyncIterable
from datetime import timedelta
from typing import Any, AsyncGenerator, Callable, Literal, cast

import nanoid
from aiochannel import Channel
from aiochannel.errors import ChannelClosed
from opentelemetry.trace import Span
from websockets.asyncio.client import ClientConnection
from websockets.exceptions import ConnectionClosed
from websockets.frames import CloseCode
from websockets.legacy.protocol import WebSocketCommonProtocol

from replit_river.error_schema import (
    ERROR_CODE_CANCEL,
    ERROR_CODE_STREAM_CLOSED,
    RiverError,
    RiverException,
    RiverServiceException,
    StreamClosedRiverServiceException,
    exception_from_message,
)
from replit_river.messages import (
    FailedSendingMessageException,
    parse_transport_msg,
)
from replit_river.rpc import (
    ACK_BIT,
    STREAM_OPEN_BIT,
    TransportMessage,
)
from replit_river.seq_manager import (
    IgnoreMessageException,
    InvalidMessageException,
    OutOfOrderMessageException,
)
from replit_river.transport_options import MAX_MESSAGE_BUFFER_SIZE, TransportOptions
from replit_river.v2.session import (
    CloseSessionCallback,
    RetryConnectionCallback,
    Session,
)

STREAM_CANCEL_BIT_TYPE = Literal[0b00100]
STREAM_CANCEL_BIT: STREAM_CANCEL_BIT_TYPE = 0b00100
STREAM_CLOSED_BIT_TYPE = Literal[0b01000]
STREAM_CLOSED_BIT: STREAM_CLOSED_BIT_TYPE = 0b01000


logger = logging.getLogger(__name__)


class ClientSession(Session):
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
        super().__init__(
            transport_id=transport_id,
            to_id=to_id,
            session_id=session_id,
            websocket=websocket,
            transport_options=transport_options,
            close_session_callback=close_session_callback,
            retry_connection_callback=retry_connection_callback,
        )

        async def do_close_websocket() -> None:
            logger.debug(
                "do_close called, _ws_connected=%r, _ws_unwrapped=%r",
                self._ws_connected,
                self._ws_unwrapped,
            )
            self._ws_connected = False
            if self._ws_unwrapped:
                self._task_manager.create_task(self._ws_unwrapped.close())
                if self._retry_connection_callback:
                    self._task_manager.create_task(self._retry_connection_callback())
            await self._begin_close_session_countdown()

        self._setup_heartbeats_task(do_close_websocket)

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

        self._task_manager.create_task(
            buffered_message_sender(
                get_ws=lambda: (
                    cast(WebSocketCommonProtocol | ClientConnection, self._ws_unwrapped)
                    if self.is_websocket_open()
                    else None
                ),
                websocket_closed_callback=self._begin_close_session_countdown,
                get_next_pending=get_next_pending,
                commit=commit,
            )
        )

    async def start_serve_responses(self) -> None:
        self._task_manager.create_task(self._serve())

    async def _serve(self) -> None:
        """Serve messages from the websocket."""
        self._reset_session_close_countdown()
        try:
            try:
                await self._handle_messages_from_ws()
            except ConnectionClosed:
                if self._retry_connection_callback:
                    self._task_manager.create_task(self._retry_connection_callback())

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

    async def _handle_messages_from_ws(self) -> None:
        while self._ws_unwrapped is None:
            await asyncio.sleep(1)
        logger.debug(
            "%s start handling messages from ws %s",
            "client",
            self._ws_unwrapped.id,
        )
        try:
            ws = self._ws_unwrapped
            async for message in ws:
                try:
                    if not self._ws_unwrapped:
                        # We should not process messages if the websocket is closed.
                        break
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

                    # Discard old messages from the buffer
                    while self._ack_buffer and self._ack_buffer[0].seq < msg.ack:
                        self._ack_buffer.popleft()

                    self._reset_session_close_countdown()

                    if msg.controlFlags & ACK_BIT != 0:
                        continue
                    stream = self._streams.get(msg.streamId)
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
        except ConnectionClosed as e:
            raise e

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
