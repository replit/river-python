import asyncio
import logging
from collections.abc import AsyncIterable, AsyncIterator
from typing import Any, Callable, Dict, Optional, Union

import msgpack  # type: ignore
import nanoid  # type: ignore
from aiochannel import Channel
from pydantic import ValidationError
from river.error_schema import RiverException
from websockets import Data
from websockets.client import WebSocketClientProtocol
from websockets.exceptions import ConnectionClosed

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


class Client:
    def __init__(self, websockets: WebSocketClientProtocol) -> None:
        self.ws = websockets
        self._tasks = set()
        self._from = nanoid.generate()
        self._streams: Dict[str, Channel[Dict[str, Any]]] = {}
        self._seq_lock = asyncio.Lock()
        self._seq = 0
        self._ack_lock = asyncio.Lock()
        self._ack = 0
        self._is_handshaked = False

        task = asyncio.create_task(self._handle_messages())
        self._tasks.add(task)

        def _handle_messages_callback(task: asyncio.Task) -> None:
            self._tasks.remove(task)
            if task.exception():
                logging.error(
                    f"Error in river.client._handle_messages: {task.exception()}"
                )

        task.add_done_callback(_handle_messages_callback)

    async def send_close_stream(
        self, service_name: str, procedure_name: str, stream_id: str
    ) -> None:
        # close stream
        await self.send_transport_message(
            self.pack_transport_message(
                from_=self._from,
                to="SERVER",
                serviceName=service_name,
                procedureName=procedure_name,
                streamId=stream_id,
                controlFlags=STREAM_CLOSED_BIT,
                payload={
                    "type": "CLOSE",
                },
            )
        )

    def unpack_all(self, byte_data):
        unpacked_objects = []
        unpacker = msgpack.Unpacker(raw=True)
        unpacker.feed(byte_data)
        for obj in unpacker:
            unpacked_objects.append(obj)
        return unpacked_objects

    def print_in_hex(self, byte_data):
        for byte in byte_data:
            print(f"0x{byte:02x}", end=" ")

    def to_transport_message(self, message: Data) -> TransportMessage:
        unpacked = msgpack.unpackb(message, timestamp=3)

        return TransportMessage(**unpacked)

    async def send_transport_message(
        self, message: TransportMessage, is_handshake: bool = False
    ) -> None:
        if not is_handshake:
            while not self._is_handshaked:
                await asyncio.sleep(0.01)

        print(
            "\n\n# sending request : ",
            message.model_dump(by_alias=True, exclude_none=True),
        )
        await self.ws.send(
            PID2_PREFIX_BYTES
            + msgpack.packb(
                message.model_dump(by_alias=True, exclude_none=True),
                datetime=True,
            )
        )
        if not is_handshake:
            async with self._seq_lock:
                self._seq += 1

    def pack_transport_message(
        self,
        from_: str,
        to: str,
        serviceName: str,
        procedureName: str,
        streamId: str,
        controlFlags: int,
        payload: Dict[str, Any],
    ) -> TransportMessage:
        return TransportMessage(
            id=nanoid.generate(),
            from_=from_,
            to=to,
            serviceName=serviceName,
            procedureName=procedureName,
            streamId=streamId,
            controlFlags=controlFlags,
            payload=payload,
            seq=self._seq,
            ack=self._ack,
        )

    def generate_nanoid(self) -> str:
        return str(nanoid.generate())

    async def _receive_pid2_message(self) -> Data:
        print("# waiting for response")
        data = await self.ws.recv()
        num_received = 1
        while data[:2] == CROSIS_PREFIX_BYTES:
            num_received += 1
            print(f"received {num_received} crosis message : {data}")
            data = await self.ws.recv()

        return data[2:]

    async def _handle_messages(self) -> None:
        handshake_request = ControlMessageHandshakeRequest(
            type="HANDSHAKE_REQ",
            protocolVersion="v1",
            instanceId="python-client-" + self.generate_nanoid(),
        )
        print(
            "# sending handshake request to server",
            handshake_request.model_dump(exclude_none=True),
        )
        await self.send_transport_message(
            TransportMessage(
                id=self.generate_nanoid(),
                from_=self._from,
                to="SERVER",
                seq=0,
                ack=0,
                serviceName=None,
                procedureName=None,
                streamId=self.generate_nanoid(),
                controlFlags=0,
                payload=handshake_request.model_dump(),
            ),
            is_handshake=True,
        )
        data = await self._receive_pid2_message()
        first_message = self.to_transport_message(data)
        print(f"# first_message : {first_message}")
        try:
            handshake_response = ControlMessageHandshakeResponse(
                **first_message.payload
            )
        except ValidationError:
            logging.error("Failed to parse handshake response")
            # TODO: close the connection here
            return
        if not handshake_response.status["ok"]:
            logging.error(f"Handshake failed: {handshake_response.status['reason']}")
            # TODO: close the connection here
            return
        self._is_handshaked = True
        print("hand shake successful")

        async for message in self.ws:
            if isinstance(message, str):
                # Not something we will try to handle.
                logging.debug(
                    "ignored a message beacuse it was a text frame: %r",
                    message,
                )
                continue

            if message[:2] == CROSIS_PREFIX_BYTES:
                logging.debug("ignored a crosis message")
                continue
            message = message[2:]

            try:
                unpacked = msgpack.unpackb(message, timestamp=3)
                msg = TransportMessage(**unpacked)
                print(f"### received msg : {msg}")
                if msg.seq != self._ack:
                    logging.debug(
                        "Received out of order message: %d, expected %d",
                        msg.seq,
                        self._ack,
                    )
                    continue
                async with self._ack_lock:
                    self._ack = msg.seq + 1
                if msg.controlFlags == ACK_BIT:
                    continue

            except ConnectionClosed:
                logging.info("Connection closed")
                break

            except (
                ValidationError,
                ValueError,
                msgpack.UnpackException,
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

        msg = self.pack_transport_message(
            from_=self._from,
            to="SERVER",
            serviceName=service_name,
            procedureName=procedure_name,
            streamId=stream_id,
            controlFlags=STREAM_OPEN_BIT | STREAM_CLOSED_BIT,
            payload=request_serializer(request),
        )
        await self.send_transport_message(msg)

        # Handle potential errors during communication
        try:
            response = await output.get()
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
        num_sent_messages = 0
        if init and init_serializer:
            num_sent_messages += 1
            msg = self.pack_transport_message(
                from_=self._from,
                to="SERVER",
                serviceName=service_name,
                procedureName=procedure_name,
                streamId=stream_id,
                controlFlags=STREAM_OPEN_BIT,
                payload=init_serializer(init),
            )
            await self.send_transport_message(msg)
            first_message = False

        async for item in request:
            control_flags = 0
            if first_message:
                control_flags = STREAM_OPEN_BIT
                first_message = False
            num_sent_messages += 1
            msg = self.pack_transport_message(
                from_=self._from,
                to="SERVER",
                serviceName=service_name,
                procedureName=procedure_name,
                streamId=stream_id,
                controlFlags=control_flags,
                payload=request_serializer(item),
            )
            await self.send_transport_message(msg)
        num_sent_messages += 1
        await self.send_close_stream(service_name, procedure_name, stream_id)

        # Handle potential errors during communication
        try:
            response = await output.get()
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
        msg = self.pack_transport_message(
            from_=self._from,
            to="SERVER",
            serviceName=service_name,
            procedureName=procedure_name,
            streamId=stream_id,
            controlFlags=STREAM_OPEN_BIT,
            payload=request_serializer(request),
        )
        await self.send_transport_message(msg)

        # Handle potential errors during communication
        try:
            async for item in output:
                print(f"# item : {item}")
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
        num_sent_messages = 0

        if init and init_serializer:
            num_sent_messages += 1
            msg = self.pack_transport_message(
                from_=self._from,
                to="SERVER",
                serviceName=service_name,
                procedureName=procedure_name,
                streamId=stream_id,
                controlFlags=STREAM_OPEN_BIT,
                payload=init_serializer(init),
            )
            await self.send_transport_message(msg)
        else:
            num_sent_messages += 1
            # Get the very first message to open the stream
            request_iter = aiter(request)
            first = await anext(request_iter)
            msg = self.pack_transport_message(
                from_=self._from,
                to="SERVER",
                serviceName=service_name,
                procedureName=procedure_name,
                streamId=stream_id,
                controlFlags=STREAM_OPEN_BIT,
                payload=request_serializer(first),
            )
            await self.send_transport_message(msg)

        # Create the encoder task
        async def _encode_stream() -> None:
            async for item in request:
                nonlocal num_sent_messages
                num_sent_messages += 1
                msg = self.pack_transport_message(
                    from_=self._from,
                    to="SERVER",
                    serviceName=service_name,
                    procedureName=procedure_name,
                    streamId=stream_id,
                    controlFlags=0,
                    payload=request_serializer(item),
                )
                await self.send_transport_message(msg)
            num_sent_messages += 1
            await self.send_close_stream(service_name, procedure_name, stream_id)

        task = asyncio.create_task(_encode_stream())
        self._tasks.add(task)
        task.add_done_callback(lambda _: self._tasks.remove(task))

        # Handle potential errors during communication
        try:
            async for item in output:
                print("send stream item in output : ", item)
                if "type" in item and item["type"] == "CLOSE":
                    # close the stream here
                    self._streams[stream_id].close()
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
