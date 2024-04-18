import logging

import msgpack  # type: ignore
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
    IgnoreTransportMessageException,
    InvalidTransportMessageException,
)
from replit_river.transport_options import TransportOptions


class FailedSendingMessageException(Exception):
    pass


PROTOCOL_VERSION = "v1"

CROSIS_PREFIX_BYTES = b"\x00\x00"
PID2_PREFIX_BYTES = b"\xff\xff"


async def send_transport_message(
    msg: TransportMessage, ws: WebSocketCommonProtocol, prefix_bytes: bytes = b""
) -> None:
    logging.debug("sent a message %r", msg)
    try:
        await ws.send(
            prefix_bytes
            + msgpack.packb(
                msg.model_dump(by_alias=True, exclude_none=True), datetime=True
            )
        )
    except websockets.exceptions.ConnectionClosed as e:
        raise e
    except Exception as e:
        raise FailedSendingMessageException(f"Exception during send message : {e}")


def formatted_bytes(message: bytes) -> str:
    return " ".join(f"{b:02x}" for b in message)


def parse_transport_msg(
    message: str | bytes, transport_options: TransportOptions
) -> TransportMessage:
    if isinstance(message, str):
        raise IgnoreTransportMessageException(
            "ignored a message beacuse it was a text frame: %r", message
        )
    if transport_options.use_prefix_bytes:
        if message.startswith(CROSIS_PREFIX_BYTES):
            raise IgnoreTransportMessageException("Skip crosis message")
        elif message.startswith(PID2_PREFIX_BYTES):
            message = message[len(PID2_PREFIX_BYTES) :]
        else:
            raise InvalidTransportMessageException(
                "Got message without prefix bytes: " f"{formatted_bytes(message)}"
            )
    try:
        unpacked_message = msgpack.unpackb(message, timestamp=3)
    except (msgpack.UnpackException, msgpack.exceptions.ExtraData):
        raise InvalidTransportMessageException("received non-msgpack message")
    try:
        msg = TransportMessage(**unpacked_message)
    except (
        ValidationError,
        ValueError,
        msgpack.UnpackException,
        PydanticCoreValidationError,
    ):
        raise InvalidTransportMessageException(
            f"failed to parse message:{message.decode()}"
        )
    return msg
