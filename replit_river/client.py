import asyncio
import logging
from collections.abc import AsyncIterable, AsyncIterator
from typing import Any, Callable, Dict, Optional, Union

import msgpack  # type: ignore
import nanoid  # type: ignore
from aiochannel import Channel
from pydantic import ValidationError
from websockets import ConnectionClosedError, Data
from websockets.client import WebSocketClientProtocol
from websockets.exceptions import ConnectionClosed

from replit_river.error_schema import ERROR_CODE_STREAM_CLOSED, RiverException
from replit_river.seq_manager import (
    IgnoreTransportMessageException,
    InvalidTransportMessageException,
    SeqManager,
)
from replit_river.task_manager import BackgroundTaskManager
from replit_river.transport import FailedSendingMessageException

from .rpc import (
    ACK_BIT,
    STREAM_CLOSED_BIT,
    STREAM_OPEN_BIT,
    ControlMessageHandshakeRequest,
    ControlMessageHandshakeResponse,
    ErrorType,
    InitType,
    RequestType,
    ResponseType,
    TransportMessage,
)

CROSIS_PREFIX_BYTES = b"\x00\x00"
PID2_PREFIX_BYTES = b"\xff\xff"
HEART_BEAT_INTERVAL_SECS = 2


class Client:
    def __init__(
        self,
        websockets: WebSocketClientProtocol,
        use_prefix_bytes: bool = True,
        client_id: Optional[str] = None,
        server_id: Optional[str] = None,
    ) -> None:
        self.ws = websockets
        self._from = nanoid.generate()
        self._streams: Dict[str, Channel[Dict[str, Any]]] = {}
        self._seq_manager = SeqManager()
        self._is_handshaked = False
        self._use_prefix_bytes = use_prefix_bytes
        self._client_id = client_id or "python-client-" + self.generate_nanoid()
        self._server_id = server_id or "SERVER"
        self._background_task_manager = BackgroundTaskManager()
        asyncio.create_task(self._start_handle_messages())

    async def _start_handle_messages(self) -> None:
        await self._background_task_manager.create_task(self._handle_messages())

    async def send_close_stream(
        self, service_name: str, procedure_name: str, stream_id: str
    ) -> None:
        # close stream
        await self.send_transport_message(
            from_=self._from,
            to=self._server_id,
            serviceName=service_name,
            procedureName=procedure_name,
            streamId=stream_id,
            controlFlags=STREAM_CLOSED_BIT,
            payload={
                "type": "CLOSE",
            },
        )

    async def _heartbeat(
        self,
        msg: TransportMessage,
    ) -> None:
        logging.debug("Start heartbeat")
        while True:
            await asyncio.sleep(HEART_BEAT_INTERVAL_SECS)
            try:
                await self.send_transport_message(
                    self._client_id,
                    self._server_id,
                    None,
                    None,
                    "heartbeat",
                    ACK_BIT,
                    {
                        "ack": msg.id,
                    },
                )
            except ConnectionClosedError:
                logging.debug("heartbeat failed")
                return

    def to_transport_message(self, message: Data) -> TransportMessage:
        unpacked = msgpack.unpackb(message, timestamp=3)

        return TransportMessage(**unpacked)

    async def send_transport_message(
        self,
        from_: str,
        to: str,
        serviceName: Optional[str],
        procedureName: Optional[str],
        streamId: str,
        controlFlags: int,
        payload: Dict[str, Any],
        is_handshake: bool = False,
    ) -> None:
        current_seq = 0
        if not is_handshake:
            while not self._is_handshaked:
                await asyncio.sleep(0.01)
        if is_handshake:
            current_seq = await self._seq_manager.get_seq()
        else:
            current_seq = await self._seq_manager.get_seq_and_increment()
        current_ack = await self._seq_manager.get_ack()
        message = TransportMessage(
            id=nanoid.generate(),
            from_=from_,
            to=to,
            serviceName=serviceName,
            procedureName=procedureName,
            streamId=streamId,
            controlFlags=controlFlags,
            payload=payload,
            seq=current_seq,
            ack=current_ack,
        )
        prefix = PID2_PREFIX_BYTES if self._use_prefix_bytes else b""
        try:
            await self.ws.send(
                prefix
                + msgpack.packb(
                    message.model_dump(by_alias=True, exclude_none=True),
                    datetime=True,
                )
            )
        except ConnectionClosed:
            raise FailedSendingMessageException(
                "Connection closed while sending message"
            )

    def generate_nanoid(self) -> str:
        return str(nanoid.generate())

    async def _receive_pid2_message(self) -> Data:
        data = await self.ws.recv()
        if self._use_prefix_bytes:
            while data[:2] == CROSIS_PREFIX_BYTES:
                data = await self.ws.recv()
            return data[2:]
        return data

    async def _handle_messages(self) -> None:
        handshake_request = ControlMessageHandshakeRequest(
            type="HANDSHAKE_REQ",
            protocolVersion="v1",
            instanceId=self._client_id,
        )
        try:
            await self.send_transport_message(
                from_=self._from,
                to=self._server_id,
                serviceName=None,
                procedureName=None,
                streamId=self.generate_nanoid(),
                controlFlags=0,
                payload=handshake_request.model_dump(),
                is_handshake=True,
            )
        except FailedSendingMessageException:
            raise RiverException(
                ERROR_CODE_STREAM_CLOSED, "Stream closed before response"
            )
        data = await self._receive_pid2_message()
        first_message = self.to_transport_message(data)
        try:
            handshake_response = ControlMessageHandshakeResponse(
                **first_message.payload
            )
        except ValidationError:
            logging.error("Failed to parse handshake response")
            # TODO: close the connection here
            return
        if not handshake_response.status.ok:
            logging.error(f"Handshake failed: {handshake_response.status.reason}")
            # TODO: close the connection here
            return
        self._is_handshaked = True
        await self._background_task_manager.create_task(self._heartbeat(first_message))

        try:
            async for message in self.ws:
                if isinstance(message, str):
                    # Not something we will try to handle.
                    logging.debug(
                        "ignored a message beacuse it was a text frame: %r",
                        message,
                    )
                    continue
                if self._use_prefix_bytes:
                    if message[:2] == CROSIS_PREFIX_BYTES:
                        logging.debug("ignored a crosis message")
                        continue
                    message = message[2:]

                try:
                    unpacked = msgpack.unpackb(message, timestamp=3)
                    msg = TransportMessage(**unpacked)
                    try:
                        await self._seq_manager.check_seq_and_update(msg)
                    except IgnoreTransportMessageException:
                        continue
                    except InvalidTransportMessageException:
                        return
                    if msg.controlFlags == ACK_BIT:
                        continue

                except ConnectionClosed:
                    logging.info("Connection closed")
                    break

                except (
                    ValidationError,
                    ValueError,
                    msgpack.UnpackException,
                    msgpack.exceptions.ExtraData,
                ):
                    logging.exception("failed to parse message")
                    return
                previous_output = self._streams.get(msg.streamId, None)
                if not previous_output:
                    logging.warning("no stream for %s", msg.streamId)
                    continue
                await previous_output.put(msg.payload)
                if msg.controlFlags & STREAM_CLOSED_BIT != 0:
                    logging.info("Closing stream %s", msg.streamId)
                    previous_output.close()
                    del self._streams[msg.streamId]
        except asyncio.CancelledError:
            logging.debug("Client loop was successfully cancelled", exc_info=False)
        finally:
            await self._background_task_manager.cancel_all_tasks()

    async def send_rpc(
        self,
        service_name: str,
        procedure_name: str,
        request: RequestType,
        request_serializer: Callable[[RequestType], Any],
        response_deserializer: Callable[[Any], ResponseType],
        error_deserializer: Callable[[Any], ErrorType],
    ) -> ResponseType:
        """Sends a single RPC request to the server.

        Expects the input and output be messages that will be msgpacked.
        """

        stream_id = nanoid.generate()
        output: Channel[Any] = Channel(1)
        self._streams[stream_id] = output
        try:
            await self.send_transport_message(
                from_=self._from,
                to=self._server_id,
                serviceName=service_name,
                procedureName=procedure_name,
                streamId=stream_id,
                controlFlags=STREAM_OPEN_BIT | STREAM_CLOSED_BIT,
                payload=request_serializer(request),
            )
        except FailedSendingMessageException:
            raise RiverException(
                ERROR_CODE_STREAM_CLOSED, "Stream closed before response"
            )

        # Handle potential errors during communication
        try:
            try:
                response = await output.get()
            except RuntimeError:
                # if the stream is closed before we get a response, we will get a
                # RuntimeError: RuntimeError: Event loop is closed
                raise RiverException(
                    ERROR_CODE_STREAM_CLOSED, "Stream closed before response"
                )
            if not response.get("ok", False):
                try:
                    error = error_deserializer(response["payload"])
                except Exception as e:
                    raise RiverException("error_deserializer", str(e))
                raise RiverException(error.code, error.message)
            return response_deserializer(response["payload"])
        except RiverException as e:
            raise e
        except Exception as e:
            # Log the error and return an appropriate error response
            logging.exception("Error during RPC communication")
            raise e

    async def send_upload(
        self,
        service_name: str,
        procedure_name: str,
        init: Optional[InitType],
        request: AsyncIterable[RequestType],
        init_serializer: Optional[Callable[[InitType], Any]],
        request_serializer: Callable[[RequestType], Any],
        response_deserializer: Callable[[Any], ResponseType],
        error_deserializer: Callable[[Any], ErrorType],
    ) -> ResponseType:
        """Sends an upload request to the server.

        Expects the input and output be messages that will be msgpacked.
        """

        stream_id = nanoid.generate()
        output: Channel[Any] = Channel(1024)
        self._streams[stream_id] = output
        first_message = True
        try:
            if init and init_serializer:
                await self.send_transport_message(
                    from_=self._from,
                    to=self._server_id,
                    serviceName=service_name,
                    procedureName=procedure_name,
                    streamId=stream_id,
                    controlFlags=STREAM_OPEN_BIT,
                    payload=init_serializer(init),
                )
                first_message = False

            async for item in request:
                control_flags = 0
                if first_message:
                    control_flags = STREAM_OPEN_BIT
                    first_message = False
                await self.send_transport_message(
                    from_=self._from,
                    to=self._server_id,
                    serviceName=service_name,
                    procedureName=procedure_name,
                    streamId=stream_id,
                    controlFlags=control_flags,
                    payload=request_serializer(item),
                )
        except FailedSendingMessageException:
            raise RiverException(
                ERROR_CODE_STREAM_CLOSED, "Stream closed before response"
            )
        await self.send_close_stream(service_name, procedure_name, stream_id)

        # Handle potential errors during communication
        try:
            try:
                response = await output.get()
            except RuntimeError:
                # if the stream is closed before we get a response, we will get a
                # RuntimeError: RuntimeError: Event loop is closed
                raise RiverException(
                    ERROR_CODE_STREAM_CLOSED, "Stream closed before response"
                )
            if not response.get("ok", False):
                try:
                    error = error_deserializer(response["payload"])
                except Exception as e:
                    raise RiverException("error_deserializer", str(e))
                raise RiverException(error.code, error.message)

            return response_deserializer(response["payload"])
        except RiverException as e:
            raise e
        except Exception as e:
            # Log the error and return an appropriate error response
            logging.exception("Error during upload communication")
            raise e

    async def send_subscription(
        self,
        service_name: str,
        procedure_name: str,
        request: RequestType,
        request_serializer: Callable[[RequestType], Any],
        response_deserializer: Callable[[Any], ResponseType],
        error_deserializer: Callable[[Any], ErrorType],
    ) -> AsyncIterator[Union[ResponseType, ErrorType]]:
        """Sends a subscription request to the server.

        Expects the input and output be messages that will be msgpacked.
        """
        stream_id = nanoid.generate()
        output: Channel[Any] = Channel(1024)
        self._streams[stream_id] = output
        try:
            await self.send_transport_message(
                from_=self._from,
                to=self._server_id,
                serviceName=service_name,
                procedureName=procedure_name,
                streamId=stream_id,
                controlFlags=STREAM_OPEN_BIT,
                payload=request_serializer(request),
            )
        except FailedSendingMessageException:
            raise RiverException(
                ERROR_CODE_STREAM_CLOSED, "Stream closed before response"
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
                        logging.exception(
                            f"Error during subscription error deserialization: {item}"
                        )
                    continue
                yield response_deserializer(item["payload"])
        except Exception as e:
            # Log the error and yield an appropriate error response
            logging.exception(f"Error during subscription communication : {item}")
            raise e

    async def send_stream(
        self,
        service_name: str,
        procedure_name: str,
        init: Optional[InitType],
        request: AsyncIterable[RequestType],
        init_serializer: Optional[Callable[[InitType], Any]],
        request_serializer: Callable[[RequestType], Any],
        response_deserializer: Callable[[Any], ResponseType],
        error_deserializer: Callable[[Any], ErrorType],
    ) -> AsyncIterator[Union[ResponseType, ErrorType]]:
        """Sends a subscription request to the server.

        Expects the input and output be messages that will be msgpacked.
        """

        stream_id = nanoid.generate()
        output: Channel[Any] = Channel(1024)
        self._streams[stream_id] = output
        try:
            if init and init_serializer:
                await self.send_transport_message(
                    from_=self._from,
                    to=self._server_id,
                    serviceName=service_name,
                    procedureName=procedure_name,
                    streamId=stream_id,
                    controlFlags=STREAM_OPEN_BIT,
                    payload=init_serializer(init),
                )
            else:
                # Get the very first message to open the stream
                request_iter = aiter(request)
                first = await anext(request_iter)
                await self.send_transport_message(
                    from_=self._from,
                    to=self._server_id,
                    serviceName=service_name,
                    procedureName=procedure_name,
                    streamId=stream_id,
                    controlFlags=STREAM_OPEN_BIT,
                    payload=request_serializer(first),
                )

        except FailedSendingMessageException:
            raise RiverException(
                ERROR_CODE_STREAM_CLOSED, "Stream closed before response"
            )

        # Create the encoder task
        async def _encode_stream() -> None:
            async for item in request:
                if item is None:
                    continue
                await self.send_transport_message(
                    from_=self._from,
                    to=self._server_id,
                    serviceName=service_name,
                    procedureName=procedure_name,
                    streamId=stream_id,
                    controlFlags=0,
                    payload=request_serializer(item),
                )
            await self.send_close_stream(service_name, procedure_name, stream_id)

        await self._background_task_manager.create_task(_encode_stream())

        # Handle potential errors during communication
        try:
            async for item in output:
                if "type" in item and item["type"] == "CLOSE":
                    break
                if not item.get("ok", False):
                    try:
                        yield error_deserializer(item["payload"])
                    except Exception:
                        logging.exception(
                            f"Error during subscription error deserialization: {item}"
                        )
                    continue
                yield response_deserializer(item["payload"])
        except Exception as e:
            # Log the error and yield an appropriate error response
            logging.exception("Error during stream communication")
            raise e
