import logging
from collections.abc import AsyncIterable, Awaitable, Callable
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, AsyncGenerator, Generator, Generic, Literal

from opentelemetry import trace
from opentelemetry.trace import Span, SpanKind, Status, StatusCode

from replit_river.error_schema import RiverError, RiverException
from replit_river.transport_options import (
    HandshakeMetadataType,
    TransportOptions,
    UriAndMetadata,
)
from replit_river.v2.client_transport import ClientTransport

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

    async def send_rpc[R, A](
        self,
        service_name: str,
        procedure_name: str,
        request: R,
        request_serializer: Callable[[R], Any],
        response_deserializer: Callable[[Any], A],
        error_deserializer: Callable[[Any], RiverError],
        timeout: timedelta,
    ) -> A:
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

    async def send_upload[I, R, A](
        self,
        service_name: str,
        procedure_name: str,
        init: I,
        request: AsyncIterable[R],
        init_serializer: Callable[[I], Any],
        request_serializer: Callable[[R], Any],
        response_deserializer: Callable[[Any], A],
        error_deserializer: Callable[[Any], RiverError],
    ) -> A:
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

    async def send_subscription[R, E, A](
        self,
        service_name: str,
        procedure_name: str,
        request: R,
        request_serializer: Callable[[R], Any],
        response_deserializer: Callable[[Any], A],
        error_deserializer: Callable[[Any], E],
    ) -> AsyncGenerator[A | E, None]:
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

    async def send_stream[I, R, E, A](
        self,
        service_name: str,
        procedure_name: str,
        init: I,
        request: AsyncIterable[R] | None,
        init_serializer: Callable[[I], Any],
        request_serializer: Callable[[R], Any] | None,
        response_deserializer: Callable[[Any], A],
        error_deserializer: Callable[[Any], E],
    ) -> AsyncGenerator[A | E, None]:
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
        status: Status | StatusCode,
        description: str | None = None,
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
        span_handle.set_status(StatusCode.OK)
    except GeneratorExit:
        # This error indicates the caller is done with the async generator
        # but messages are still left. This is okay, we do not consider it an error.
        span_handle.set_status(StatusCode.OK)
        raise
    except RiverException as e:
        span.record_exception(e, escaped=True)
        _record_river_error(span_handle, RiverError(code=e.code, message=e.message))
        raise
    except BaseException as e:
        span.record_exception(e, escaped=True)
        span_handle.set_status(StatusCode.ERROR, f"{type(e).__name__}: {e}")
        raise
    finally:
        span.end()


def _record_river_error(span_handle: _SpanHandle, error: RiverError) -> None:
    span_handle.set_status(StatusCode.ERROR, error.message)
    span_handle.span.record_exception(RiverException(error.code, error.message))
    span_handle.span.set_attribute("river.error_code", error.code)
    span_handle.span.set_attribute("river.error_message", error.message)
