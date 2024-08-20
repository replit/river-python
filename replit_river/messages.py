import logging
from typing import Any, Callable, Coroutine

import msgpack
import websockets
from pydantic import ValidationError
from pydantic_core import ValidationError as PydanticCoreValidationError
from websockets import (
    WebSocketCommonProtocol,
)

from replit_river.rpc import (
    TransportMessage,
)
from replit_river.seq_manager import (
    IgnoreMessageException,
    InvalidMessageException,
)
from replit_river.transport_options import TransportOptions

logger = logging.getLogger(__name__)


class WebsocketClosedException(Exception):
    pass


class FailedSendingMessageException(Exception):
    pass


PROTOCOL_VERSION = "v1.1"

CROSIS_PREFIX_BYTES = b"\x00\x00"
PID2_PREFIX_BYTES = b"\xff\xff"


async def send_transport_message(
    msg: TransportMessage,
    ws: WebSocketCommonProtocol,
    websocket_closed_callback: Callable[[], Coroutine[Any, Any, None]],
    prefix_bytes: bytes = b"",
) -> None:
    logger.debug("sending a message %r to ws %s", msg, ws)
    try:
        await ws.send(
            prefix_bytes
            + msgpack.packb(
                msg.model_dump(by_alias=True, exclude_none=True), datetime=True
            )
        )
    except websockets.exceptions.ConnectionClosed as e:
        await websocket_closed_callback()
        raise WebsocketClosedException("Websocket closed during send message") from e
    except RuntimeError as e:
        # RuntimeError: Unexpected ASGI message 'websocket.send',
        # after sending 'websocket.close'
        await websocket_closed_callback()
        raise WebsocketClosedException(
            "Websocket closed RuntimeError during send message"
        ) from e
    except Exception as e:
        raise FailedSendingMessageException("Exception during send message") from e


def formatted_bytes(message: bytes) -> str:
    return " ".join(f"{b:02x}" for b in message)


def parse_transport_msg(
    message: str | bytes, transport_options: TransportOptions
) -> TransportMessage:
    if isinstance(message, str):
        raise IgnoreMessageException(
            f"ignored a message beacuse it was a text frame: {message}"
        )
    if transport_options.use_prefix_bytes:
        if message.startswith(CROSIS_PREFIX_BYTES):
            raise IgnoreMessageException("Skip crosis message")
        elif message.startswith(PID2_PREFIX_BYTES):
            message = message[len(PID2_PREFIX_BYTES) :]
        else:
            raise InvalidMessageException(
                f"Got message without prefix bytes: {formatted_bytes(message)[:5]}"
            )
    try:
        # :param int timestamp:
        #     Control how timestamp type is unpacked:

        #         0 - Timestamp
        #         1 - float  (Seconds from the EPOCH)
        #         2 - int  (Nanoseconds from the EPOCH)
        #         3 - datetime.datetime  (UTC).
        unpacked_message = msgpack.unpackb(message, timestamp=3)
    except (msgpack.UnpackException, msgpack.exceptions.ExtraData) as e:
        raise InvalidMessageException("received non-msgpack message") from e
    try:
        msg = TransportMessage(**unpacked_message)
    except (
        ValidationError,
        ValueError,
        msgpack.UnpackException,
        PydanticCoreValidationError,
    ) as e:
        raise InvalidMessageException(
            f"failed to parse message: {unpacked_message}"
        ) from e
    return msg
