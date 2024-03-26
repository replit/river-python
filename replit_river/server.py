import asyncio
import logging
from typing import Any, Dict, Mapping, Set, Tuple

import msgpack  # type: ignore
import nanoid  # type: ignore
from aiochannel import Channel
from pydantic import ValidationError
from pydantic_core import ValidationError as PydanticCoreValidationError
from websockets.exceptions import ConnectionClosedError
from websockets.server import WebSocketServerProtocol

from .rpc import (
    ACK_BIT,
    STREAM_CLOSED_BIT,
    STREAM_OPEN_BIT,
    GenericRpcHandler,
    TransportMessage,
)


async def send_messages(
    initial_message: TransportMessage,
    ws: WebSocketServerProtocol,
    output: Channel[Any],
    is_stream: bool,
) -> None:
    """Send serialized messages to the websockets."""
    async for payload in output:
        msg = initial_message.model_copy(deep=True)
        msg.id = nanoid.generate()
        msg.from_, msg.to = msg.to, msg.from_
        if not is_stream:
            msg.controlFlags = ACK_BIT | STREAM_CLOSED_BIT
        msg.payload = payload
        logging.debug("sent a message %r", msg)
        await ws.send(msgpack.packb(msg.model_dump(by_alias=True), datetime=True))
        if not is_stream:
            # If the response is not a stream, the client expects a single
            # message back.
            return
    msg = initial_message.model_copy(deep=True)
    msg.id = nanoid.generate()
    msg.from_, msg.to = msg.to, msg.from_
    msg.controlFlags = STREAM_CLOSED_BIT
    msg.payload = {}
    logging.debug("sent an end of stream", msg)
    await ws.send(msgpack.packb(msg.model_dump(by_alias=True)))


class Server(object):
    def __init__(self) -> None:
        self._handlers: Dict[Tuple[str, str], Tuple[str, GenericRpcHandler]] = {}

    def add_rpc_handlers(
        self,
        rpc_handlers: Mapping[Tuple[str, str], Tuple[str, GenericRpcHandler]],
    ) -> None:
        self._handlers.update(rpc_handlers)

    async def serve(self, websocket: WebSocketServerProtocol) -> None:
        logging.debug("got a client")
        background_tasks: Set[asyncio.Task] = set()
        streams: Dict[str, Channel[Any]] = {}
        try:
            async with asyncio.TaskGroup() as tg:
                try:
                    async for message in websocket:
                        if isinstance(message, str):
                            # Not something we will try to handle.
                            logging.debug(
                                "ignored a message beacuse it was a text frame: %r",
                                message,
                            )
                            continue
                        try:
                            unpacked_message = msgpack.unpackb(message, timestamp=3)
                        except msgpack.UnpackException:
                            logging.exception("received non-msgpack message")
                            return
                        if check_if_ack_message(unpacked_message):
                            # Ignore ack messages
                            continue
                        try:
                            msg = TransportMessage(**unpacked_message)
                        except (
                            ValidationError,
                            ValueError,
                            msgpack.UnpackException,
                            PydanticCoreValidationError,
                        ):
                            logging.exception(
                                f"failed to parse message:{message.decode()}"
                            )
                            return

                        logging.debug("got a message %r", msg)
                        previous_input = streams.get(msg.streamId, None)
                        if previous_input:
                            # River has a bug where it sends all messages
                            # in a stream with the open bit.
                            msg.controlFlags &= ~STREAM_OPEN_BIT
                        if msg.controlFlags & STREAM_OPEN_BIT != 0:
                            if not msg.serviceName or not msg.procedureName:
                                logging.warning(
                                    "no service or procedure name in %r", msg
                                )
                                return
                            key = (msg.serviceName, msg.procedureName)
                            handler = self._handlers.get(key, None)
                            if not handler:
                                logging.exception(
                                    "No handler for %s handlers : "
                                    f"{self._handlers.keys()}",
                                    key,
                                )
                                return
                            method_type, handler_func = handler
                            is_output_stream = method_type in (
                                "subscription-stream",
                                "stream",
                            )
                            # New channel pair.
                            input: Channel[Any] = Channel(1)
                            output: Channel[Any] = Channel(1)
                            await input.put(msg.payload)
                            if msg.controlFlags & STREAM_CLOSED_BIT != 0:
                                input.close()
                            else:
                                # We'll need to save it for later.
                                streams[msg.streamId] = input

                                ack_msg = msg.model_copy(deep=True)
                                ack_msg.id = nanoid.generate()
                                ack_msg.from_, ack_msg.to = ack_msg.to, ack_msg.from_
                                ack_msg.controlFlags = ACK_BIT
                                ack_msg.payload = {
                                    "ack": msg.id,
                                }
                                await websocket.send(
                                    msgpack.packb(
                                        ack_msg.model_dump(by_alias=True), datetime=True
                                    )
                                )

                            def remove(task_to_remove: asyncio.Task[Any]) -> None:
                                if task_to_remove in background_tasks:
                                    background_tasks.remove(task_to_remove)
                                try:
                                    exception = task_to_remove.exception()
                                except asyncio.CancelledError:
                                    logging.debug("Task was cancelled", exc_info=True)
                                    return
                                except Exception:
                                    logging.error(
                                        "Error retrieving task exception", exc_info=True
                                    )
                                    return
                                if exception:
                                    logging.error(
                                        "Task resulted in an exception",
                                        exc_info=exception,
                                    )

                            task1 = tg.create_task(
                                handler_func(msg.from_, input, output)
                            )
                            background_tasks.add(task1)
                            task1.add_done_callback(remove)
                            task2 = tg.create_task(
                                send_messages(msg, websocket, output, is_output_stream)
                            )
                            background_tasks.add(task2)
                            task2.add_done_callback(remove)

                        else:
                            if not previous_input:
                                logging.warning("no stream for %s", msg.streamId)
                                continue

                            ack_msg = msg.model_copy(deep=True)
                            ack_msg.id = nanoid.generate()
                            ack_msg.from_, ack_msg.to = ack_msg.to, ack_msg.from_
                            ack_msg.controlFlags = ACK_BIT
                            ack_msg.payload = {
                                "ack": msg.id,
                            }
                            await websocket.send(
                                msgpack.packb(
                                    ack_msg.model_dump(by_alias=True), datetime=True
                                )
                            )

                            if msg.controlFlags & STREAM_CLOSED_BIT == 0:
                                await previous_input.put(msg.payload)
                            else:
                                previous_input.close()
                                del streams[msg.streamId]
                except ConnectionClosedError:
                    # This is fine, but close all streams.
                    for previous_input in streams.values():
                        previous_input.close()
                    streams.clear()
                except Exception:
                    logging.exception("caught exception at message iterator")
                finally:
                    logging.debug("closing websocket")
                    for task in background_tasks:
                        task.cancel()
                    await websocket.close()
        except Exception:
            logging.exception("caught exception at task group")


def check_if_ack_message(unpacked_message: Dict[str, Any]) -> bool:
    if unpacked_message is None:
        return False
    return "ack" in unpacked_message.get("payload", {})
