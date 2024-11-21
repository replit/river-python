import logging
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable
from contextlib import contextmanager
from typing import Any, Generator, Generic, Literal, Optional, Union

from opentelemetry import trace

from replit_river.client_transport import ClientTransport
from replit_river.error_schema import RiverException
from replit_river.transport_options import (
    HandshakeMetadataType,
    TransportOptions,
    UriAndMetadata,
)

from .rpc import (
    ErrorType,
    InitType,
    RequestType,
    ResponseType,
)

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class Client(Generic[HandshakeMetadataType]):
    def __init__(
        self,
        uri_and_metadata_factory: Callable[
            [], Awaitable[UriAndMetadata[HandshakeMetadataType]]
        ],
        client_id: str,
        server_id: str,
        transport_options: TransportOptions,
    ) -> None:
        self._client_id = client_id
        self._server_id = server_id
        self._transport = ClientTransport[HandshakeMetadataType](
            uri_and_metadata_factory=uri_and_metadata_factory,
            client_id=client_id,
            server_id=server_id,
            transport_options=transport_options,
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
        with _trace_procedure("rpc", service_name, procedure_name):
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
        with _trace_procedure("upload", service_name, procedure_name):
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
        with _trace_procedure("subscription", service_name, procedure_name):
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
        with _trace_procedure("stream", service_name, procedure_name):
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


@contextmanager
def _trace_procedure(
    procedure_type: Literal["rpc", "upload", "subscription", "stream"],
    service_name: str,
    procedure_name: str,
) -> Generator[None, None, None]:
    with tracer.start_as_current_span(
        f"river.client.{procedure_type}.{service_name}.{procedure_name}",
        kind=trace.SpanKind.CLIENT,
    ) as span:
        try:
            yield
        except RiverException as e:
            span.set_attribute("river.error_code", e.code)
            span.set_attribute("river.error_message", e.message)
            raise e
