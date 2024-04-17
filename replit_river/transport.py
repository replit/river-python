import asyncio
import logging
from typing import Any, Dict, Optional, Tuple

import msgpack  # type: ignore
import nanoid  # type: ignore
import websockets
from aiochannel import Channel
from pydantic import ValidationError
from pydantic_core import ValidationError as PydanticCoreValidationError
from websockets.exceptions import ConnectionClosedError
from websockets.server import WebSocketServerProtocol

from replit_river.seq_manager import (
    IgnoreTransportMessageException,
    InvalidTransportMessageException,
    SeqManager,
)
from replit_river.task_manager import BackgroundTaskManager

from .rpc import (
    ACK_BIT,
    STREAM_CLOSED_BIT,
    STREAM_OPEN_BIT,
    ControlMessageHandshakeRequest,
    ControlMessageHandshakeResponse,
    GenericRpcHandler,
    HandShakeStatus,
    TransportMessage,
)

PROTOCOL_VERSION = "v1"
HEART_BEAT_INTERVAL_SECS = 2


class FailedSendingMessageException(Exception):
    pass


class TransportManager:
    def __init__(self) -> None:
        self._transports_by_id: Dict[str, "Transport"] = {}
        self._lock = asyncio.Lock()

    async def add_transport(self, transport_id: str, transport: "Transport") -> None:
        transport_to_close = None
        async with self._lock:
            if transport_id in self._transports_by_id:
                if (
                    self._transports_by_id[transport_id]._client_instance_id
                    != transport._client_instance_id
                ):
                    transport_to_close = self._transports_by_id[transport_id]
            self._transports_by_id[transport_id] = transport
        if transport_to_close:
            await transport_to_close.close()

    async def remove_transport(self, transport_id: str) -> None:
        transport_to_stop = None
        async with self._lock:
            if transport_id in self._transports_by_id:
                transport_to_stop = self._transports_by_id.pop(transport_id)

        if transport_to_stop:
            logging.debug("Stopping transport websocket")
            await transport_to_stop.close()


class Transport(object):
    """A transport object that handles the websocket connection with a client."""

    def __init__(
        self,
        server_instance_id: str,
        handlers: Dict[Tuple[str, str], Tuple[str, GenericRpcHandler]],
        websocket: WebSocketServerProtocol,
        transports_manager: TransportManager,
    ) -> None:
        self._server_instance_id = server_instance_id
        self._client_instance_id: Optional[str] = None
        self._handlers = handlers
        self.websocket = websocket
        self.streams: Dict[str, Channel[Any]] = {}
        self.is_handshake_success = False
        self._transports_manager = transports_manager
        self._seq_manager = SeqManager()
        self._background_task_manager = BackgroundTaskManager()

    async def send_message(
        self,
        initial_message: TransportMessage,
        ws: WebSocketServerProtocol,
        control_flags: int,
        payload: Dict,
        is_hand_shake: bool = False,
    ) -> None:
        """Send serialized messages to the websockets."""
        msg = TransportMessage(
            streamId=initial_message.streamId,
            id=nanoid.generate(),
            from_=initial_message.to,
            to=initial_message.from_,
            seq=0 if is_hand_shake else await self._seq_manager.get_seq_and_increment(),
            ack=await self._seq_manager.get_ack(),
            controlFlags=control_flags,
            payload=payload,
            serviceName=initial_message.serviceName,
            procedureName=initial_message.procedureName,
        )
        logging.debug("sent a message %r", msg)
        try:
            await ws.send(
                msgpack.packb(
                    msg.model_dump(by_alias=True, exclude_none=True), datetime=True
                )
            )
        except websockets.exceptions.ConnectionClosedOK:
            logging.warning(
                "Trying to send message while connection closed "
                f"for between server : {self._server_instance_id} and "
                f"client : {self._client_instance_id}"
            )
            raise FailedSendingMessageException()

    async def send_responses(
        self,
        initial_message: TransportMessage,
        ws: WebSocketServerProtocol,
        output: Channel[Any],
        is_stream: bool,
    ) -> None:
        """Send serialized messages to the websockets."""
        logging.debug("sent response of stream %r", initial_message.streamId)
        async for payload in output:
            if not is_stream:
                await self.send_message(initial_message, ws, STREAM_CLOSED_BIT, payload)
                return
            await self.send_message(initial_message, ws, 0, payload)
        logging.debug("sent an end of stream %r", initial_message.streamId)
        await self.send_message(
            initial_message, ws, STREAM_CLOSED_BIT, {"type": "CLOSE"}
        )

    async def _process_handshake_request_message(
        self, transport_message: TransportMessage, websocket: WebSocketServerProtocol
    ) -> ControlMessageHandshakeRequest:
        """Returns the instance id instance id."""
        try:
            handshake_request = ControlMessageHandshakeRequest(
                **transport_message.payload
            )
        except (ValidationError, ValueError):
            response_message = ControlMessageHandshakeResponse(
                status=HandShakeStatus(
                    ok=False, reason="failed validate handshake request"
                )
            )
            await self.send_message(
                transport_message,
                websocket,
                0,
                response_message.model_dump(by_alias=True, exclude_none=True),
                is_hand_shake=True,
            )
            logging.exception("failed to parse handshake request")
            raise InvalidTransportMessageException("failed validate handshake request")

        if handshake_request.protocolVersion != PROTOCOL_VERSION:
            response_message = ControlMessageHandshakeResponse(
                status=HandShakeStatus(ok=False, reason="protocol version mismatch")
            )
            await self.send_message(
                transport_message,
                websocket,
                0,
                response_message.model_dump(by_alias=True, exclude_none=True),
                is_hand_shake=True,
            )
            error_str = (
                "protocol version mismatch: "
                + f"{handshake_request.protocolVersion} != {PROTOCOL_VERSION}"
            )
            logging.error(error_str)
            raise InvalidTransportMessageException(error_str)

        response_message = ControlMessageHandshakeResponse(
            status=HandShakeStatus(ok=True, instanceId=self._server_instance_id)
        )
        await self.send_message(
            transport_message,
            websocket,
            0,
            response_message.model_dump(by_alias=True, exclude_none=True),
            is_hand_shake=True,
        )
        return handshake_request

    def _formatted_bytes(self, message: bytes) -> str:
        return " ".join(f"{b:02x}" for b in message)

    def _parse_transport_msg(self, message: str | bytes) -> TransportMessage:
        if isinstance(message, str):
            logging.debug(
                "ignored a message beacuse it was a text frame: %r",
                message,
            )
            raise IgnoreTransportMessageException()
        try:
            unpacked_message = msgpack.unpackb(message, timestamp=3)
        except (msgpack.UnpackException, msgpack.exceptions.ExtraData):
            logging.exception("received non-msgpack message")
            raise InvalidTransportMessageException()
        try:
            msg = TransportMessage(**unpacked_message)
        except (
            ValidationError,
            ValueError,
            msgpack.UnpackException,
            PydanticCoreValidationError,
        ):
            logging.exception(f"failed to parse message:{message.decode()}")
            raise InvalidTransportMessageException()
        return msg

    async def _establish_handshake(
        self, msg: TransportMessage, websocket: WebSocketServerProtocol
    ) -> None:
        try:
            handshake_request = await self._process_handshake_request_message(
                msg, websocket
            )
            self._client_instance_id = handshake_request.instanceId
        except InvalidTransportMessageException:
            raise
        transport_id = msg.from_
        await self._transports_manager.add_transport(transport_id, self)

    async def _heartbeat(
        self,
        msg: TransportMessage,
        websocket: WebSocketServerProtocol,
    ) -> None:
        logging.debug("Start heartbeat")
        while True:
            await asyncio.sleep(HEART_BEAT_INTERVAL_SECS)
            try:
                await self.send_message(
                    msg,
                    websocket,
                    ACK_BIT,
                    {
                        "ack": msg.id,
                    },
                )
            except ConnectionClosedError:
                logging.debug("heartbeat failed")
                return

    async def handle_messages_from_ws(
        self, websocket: WebSocketServerProtocol, tg: asyncio.TaskGroup
    ) -> None:
        async for message in websocket:
            try:
                msg = self._parse_transport_msg(message)
            except IgnoreTransportMessageException:
                continue
            except InvalidTransportMessageException:
                logging.error("Got invalid transport message, closing connection")
                return

            logging.debug("got a message %r", msg)

            if not self.is_handshake_success:
                try:
                    await self._establish_handshake(msg, websocket)
                    self.is_handshake_success = True
                    await self._background_task_manager.create_task(
                        self._heartbeat(msg, websocket), tg
                    )
                    logging.debug(
                        "handshake success for client_instance_id :"
                        f" {self._client_instance_id}"
                    )

                    continue
                except InvalidTransportMessageException:
                    logging.error("Got invalid transport message, closing connection")
                    return

            try:
                await self._seq_manager.check_seq_and_update(msg)
            except IgnoreTransportMessageException:
                continue
            except InvalidTransportMessageException:
                return
            if msg.controlFlags & ACK_BIT != 0:
                # Ignore ack messages.
                continue

            stream = self.streams.get(msg.streamId, None)
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
                output_stream: Channel[Any] = Channel(
                    1024 if is_streaming_output else 1
                )
                await input_stream.put(msg.payload)
                if not stream:
                    # We'll need to save it for later.
                    self.streams[msg.streamId] = input_stream
                # Start the handler.
                await self._background_task_manager.create_task(
                    handler_func(msg.from_, input_stream, output_stream), tg
                )
                await self._background_task_manager.create_task(
                    self.send_responses(
                        msg, websocket, output_stream, is_streaming_output
                    ),
                    tg,
                )

            else:
                # messages after stream is opened
                if not stream:
                    logging.warning("no stream for %s", msg.streamId)
                    continue
                if not (
                    msg.controlFlags & STREAM_CLOSED_BIT != 0
                    and msg.payload.get("type", None) == "CLOSE"
                ):
                    # close message is not sent to the stream
                    await stream.put(msg.payload)

            if msg.controlFlags & STREAM_CLOSED_BIT != 0:
                if stream:
                    stream.close()
                del self.streams[msg.streamId]

    async def serve(self) -> None:
        try:
            async with asyncio.TaskGroup() as tg:
                try:
                    await self.handle_messages_from_ws(self.websocket, tg)
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

    async def close(self) -> None:
        for previous_input in self.streams.values():
            previous_input.close()
        self.streams.clear()
        await self._background_task_manager.cancel_all_tasks()
        if self.websocket:
            await self.websocket.close()
