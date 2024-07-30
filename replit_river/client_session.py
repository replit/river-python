import logging
from collections.abc import AsyncIterable, AsyncIterator
from typing import Any, Callable, Optional, Union

import nanoid  # type: ignore
from aiochannel import Channel
from aiochannel.errors import ChannelClosed

from replit_river.error_schema import ERROR_CODE_STREAM_CLOSED, RiverException
from replit_river.session import Session
from replit_river.transport_options import MAX_MESSAGE_BUFFER_SIZE

from .rpc import (
    STREAM_CLOSED_BIT,
    STREAM_OPEN_BIT,
    ErrorType,
    InitType,
    RequestType,
    ResponseType,
)

logger = logging.getLogger(__name__)


class ClientSession(Session):
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
        await self.send_message(
            stream_id=stream_id,
            control_flags=STREAM_OPEN_BIT | STREAM_CLOSED_BIT,
            payload=request_serializer(request),
            service_name=service_name,
            procedure_name=procedure_name,
        )
        # Handle potential errors during communication
        try:
            try:
                response = await output.get()
            except ChannelClosed as e:
                raise RiverException(
                    ERROR_CODE_STREAM_CLOSED, "Stream closed before response"
                ) from e
            except RuntimeError as e:
                raise RiverException(ERROR_CODE_STREAM_CLOSED, str(e)) from e
            if not response.get("ok", False):
                try:
                    error = error_deserializer(response["payload"])
                except Exception as e:
                    raise RiverException("error_deserializer", str(e)) from e
                raise RiverException(error.code, error.message)
            return response_deserializer(response["payload"])
        except RiverException as e:
            raise e
        except Exception as e:
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
        output: Channel[Any] = Channel(1)
        self._streams[stream_id] = output
        first_message = True
        try:
            if init and init_serializer:
                await self.send_message(
                    stream_id=stream_id,
                    control_flags=STREAM_OPEN_BIT,
                    service_name=service_name,
                    procedure_name=procedure_name,
                    payload=init_serializer(init),
                )
                first_message = False
            # If this request is not closed and the session is killed, we should
            # throw exception here
            async for item in request:
                control_flags = 0
                if first_message:
                    control_flags = STREAM_OPEN_BIT
                    first_message = False
                await self.send_message(
                    stream_id=stream_id,
                    service_name=service_name,
                    procedure_name=procedure_name,
                    control_flags=control_flags,
                    payload=request_serializer(item),
                )
        except Exception as e:
            raise RiverException(ERROR_CODE_STREAM_CLOSED, str(e)) from e
        await self.send_close_stream(
            service_name,
            procedure_name,
            stream_id,
            extra_control_flags=STREAM_OPEN_BIT if first_message else 0,
        )

        # Handle potential errors during communication
        # TODO: throw a error when the transport is hard closed
        try:
            try:
                response = await output.get()
            except ChannelClosed as e:
                raise RiverException(
                    ERROR_CODE_STREAM_CLOSED, "Stream closed before response"
                ) from e
            except RuntimeError as e:
                raise RiverException(ERROR_CODE_STREAM_CLOSED, str(e)) from e
            if not response.get("ok", False):
                try:
                    error = error_deserializer(response["payload"])
                except Exception as e:
                    raise RiverException("error_deserializer", str(e)) from e
                raise RiverException(error.code, error.message)

            return response_deserializer(response["payload"])
        except RiverException as e:
            raise e
        except Exception as e:
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
        output: Channel[Any] = Channel(MAX_MESSAGE_BUFFER_SIZE)
        self._streams[stream_id] = output
        await self.send_message(
            service_name=service_name,
            procedure_name=procedure_name,
            stream_id=stream_id,
            control_flags=STREAM_OPEN_BIT,
            payload=request_serializer(request),
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
            raise RiverException(
                ERROR_CODE_STREAM_CLOSED, "Stream closed before response"
            ) from e
        except Exception as e:
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
        output: Channel[Any] = Channel(MAX_MESSAGE_BUFFER_SIZE)
        self._streams[stream_id] = output
        empty_stream = False
        try:
            if init and init_serializer:
                await self.send_message(
                    service_name=service_name,
                    procedure_name=procedure_name,
                    stream_id=stream_id,
                    control_flags=STREAM_OPEN_BIT,
                    payload=init_serializer(init),
                )
            else:
                # Get the very first message to open the stream
                request_iter = aiter(request)
                first = await anext(request_iter)
                await self.send_message(
                    service_name=service_name,
                    procedure_name=procedure_name,
                    stream_id=stream_id,
                    control_flags=STREAM_OPEN_BIT,
                    payload=request_serializer(first),
                )

        except StopAsyncIteration:
            empty_stream = True

        except Exception as e:
            raise RiverException(ERROR_CODE_STREAM_CLOSED, str(e)) from e

        # Create the encoder task
        async def _encode_stream() -> None:
            if empty_stream:
                await self.send_close_stream(
                    service_name,
                    procedure_name,
                    stream_id,
                    extra_control_flags=STREAM_OPEN_BIT,
                )
                return

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
            raise RiverException(
                ERROR_CODE_STREAM_CLOSED, "Stream closed before response"
            ) from e
        except Exception as e:
            raise e

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
