import logging
from collections.abc import AsyncIterable, Awaitable, Callable
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, AsyncGenerator, Generator, Generic, Literal, Optional, Union

from opentelemetry import trace
from opentelemetry.trace import Span, SpanKind, Status, StatusCode
from pydantic import (
    BaseModel,
    ValidationInfo,
)

from replit_river.client_transport import ClientTransport
from replit_river.error_schema import RiverError, RiverException
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


@dataclass(frozen=True)
class RiverUnknownValue(BaseModel):
    tag: Literal["RiverUnknownValue"]
    value: Any


def translate_unknown_value(
    value: Any, handler: Callable[[Any], Any], info: ValidationInfo
) -> Any | RiverUnknownValue:
    try:
        return handler(value)
    except Exception:
        return RiverUnknownValue(tag="RiverUnknownValue", value=value)


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
        timeout: timedelta,
    ) -> ResponseType:
        with _trace_procedure("rpc", service_name, procedure_name) as span_handle:
            session = await self._transport.get_or_create_session()
            return await session.send_rpc(
                service_name,
                procedure_name,
                request,
                request_serializer,
                response_deserializer,
                error_deserializer,
                span_handle.span,
                timeout,
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
        with _trace_procedure("upload", service_name, procedure_name) as span_handle:
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
                span_handle.span,
            )

    async def send_subscription(
        self,
        service_name: str,
        procedure_name: str,
        request: RequestType,
        request_serializer: Callable[[RequestType], Any],
        response_deserializer: Callable[[Any], ResponseType],
        error_deserializer: Callable[[Any], ErrorType],
    ) -> AsyncGenerator[Union[ResponseType, RiverError], None]:
        with _trace_procedure(
            "subscription", service_name, procedure_name
        ) as span_handle:
            session = await self._transport.get_or_create_session()
            async for msg in session.send_subscription(
                service_name,
                procedure_name,
                request,
                request_serializer,
                response_deserializer,
                error_deserializer,
                span_handle.span,
            ):
                if isinstance(msg, RiverError):
                    _record_river_error(span_handle, msg)
                yield msg  # type: ignore # https://github.com/python/mypy/issues/10817

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
    ) -> AsyncGenerator[Union[ResponseType, RiverError], None]:
        with _trace_procedure("stream", service_name, procedure_name) as span_handle:
            session = await self._transport.get_or_create_session()
            async for msg in session.send_stream(
                service_name,
                procedure_name,
                init,
                request,
                init_serializer,
                request_serializer,
                response_deserializer,
                error_deserializer,
                span_handle.span,
            ):
                if isinstance(msg, RiverError):
                    _record_river_error(span_handle, msg)
                yield msg  # type: ignore # https://github.com/python/mypy/issues/10817


@dataclass
class _SpanHandle:
    """Wraps a span and keeps track of whether or not a status has been recorded yet."""

    span: Span
    did_set_status: bool = False

    def set_status(
        self,
        status: Union[Status, StatusCode],
        description: Optional[str] = None,
    ) -> None:
        if self.did_set_status:
            return
        self.did_set_status = True
        self.span.set_status(status, description)


@contextmanager
def _trace_procedure(
    procedure_type: Literal["rpc", "upload", "subscription", "stream"],
    service_name: str,
    procedure_name: str,
) -> Generator[_SpanHandle, None, None]:
    span = tracer.start_span(
        f"river.client.{procedure_type}.{service_name}.{procedure_name}",
        kind=SpanKind.CLIENT,
    )
    span_handle = _SpanHandle(span)
    try:
        yield span_handle
    except GeneratorExit:
        # This error indicates the caller is done with the async generator
        # but messages are still left. This is okay, we do not consider it an error.
        raise
    except RiverException as e:
        span.record_exception(e, escaped=True)
        _record_river_error(span_handle, RiverError(code=e.code, message=e.message))
        raise e
    except BaseException as e:
        span.record_exception(e, escaped=True)
        span_handle.set_status(StatusCode.ERROR, f"{type(e).__name__}: {e}")
        raise e
    finally:
        span_handle.set_status(StatusCode.OK)
        span.end()


def _record_river_error(span_handle: _SpanHandle, error: RiverError) -> None:
    span_handle.set_status(StatusCode.ERROR, error.message)
    span_handle.span.record_exception(RiverException(error.code, error.message))
    span_handle.span.set_attribute("river.error_code", error.code)
    span_handle.span.set_attribute("river.error_message", error.message)
