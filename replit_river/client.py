import logging
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable
from typing import Any, Generic, Optional, TypeVar, Union

from ddtrace import tracer

from replit_river.client_transport import ClientTransport
from replit_river.transport_options import TransportOptions

from .rpc import (
    ErrorType,
    InitType,
    RequestType,
    ResponseType,
)

logger = logging.getLogger(__name__)


HandshakeType = TypeVar("HandshakeType")


class Client(Generic[HandshakeType]):
    def __init__(
        self,
        websocket_uri_factory: Callable[[], Awaitable[str]],
        client_id: str,
        server_id: str,
        transport_options: TransportOptions,
        handshake_metadata_factory: Callable[[], Awaitable[HandshakeType]],
    ) -> None:
        self._client_id = client_id
        self._server_id = server_id
        self._transport = ClientTransport[HandshakeType](
            websocket_uri_factory=websocket_uri_factory,
            client_id=client_id,
            server_id=server_id,
            transport_options=transport_options,
            handshake_metadata_factory=handshake_metadata_factory,
        )

    async def close(self) -> None:
        logger.info(f"river client {self._client_id} start closing")
        await self._transport.close()
        logger.info(f"river client {self._client_id} closed")

    async def ensure_connected(self) -> None:
        await self._transport.get_or_create_session()

    async def send_rpc(
        self,
        service_name: str,
        procedure_name: str,
        request: RequestType,
        request_serializer: Callable[[RequestType], Any],
        response_deserializer: Callable[[Any], ResponseType],
        error_deserializer: Callable[[Any], ErrorType],
    ) -> ResponseType:
        with tracer.trace("pid2.client", resource=f"{service_name}.{procedure_name}"):
            session = await self._transport.get_or_create_session()
            return await session.send_rpc(
                service_name,
                procedure_name,
                request,
                request_serializer,
                response_deserializer,
                error_deserializer,
            )

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
        with tracer.trace("pid2.client", resource=f"{service_name}.{procedure_name}"):
            session = await self._transport.get_or_create_session()
            return await session.send_upload(
                service_name,
                procedure_name,
                init,
                request,
                init_serializer,
                request_serializer,
                response_deserializer,
                error_deserializer,
            )

    async def send_subscription(
        self,
        service_name: str,
        procedure_name: str,
        request: RequestType,
        request_serializer: Callable[[RequestType], Any],
        response_deserializer: Callable[[Any], ResponseType],
        error_deserializer: Callable[[Any], ErrorType],
    ) -> AsyncIterator[Union[ResponseType, ErrorType]]:
        with tracer.trace("pid2.client", resource=f"{service_name}.{procedure_name}"):
            session = await self._transport.get_or_create_session()
            return session.send_subscription(
                service_name,
                procedure_name,
                request,
                request_serializer,
                response_deserializer,
                error_deserializer,
            )

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
        with tracer.trace("pid2.client", resource=f"{service_name}.{procedure_name}"):
            session = await self._transport.get_or_create_session()
            return session.send_stream(
                service_name,
                procedure_name,
                init,
                request,
                init_serializer,
                request_serializer,
                response_deserializer,
                error_deserializer,
            )
