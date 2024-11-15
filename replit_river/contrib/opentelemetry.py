from typing import Any, AsyncIterator, Awaitable, Callable

from opentelemetry import trace

from replit_river.client_interceptor import (
    ClientInterceptor,
    ClientRpcDetails,
    ClientStreamDetails,
    ClientSubscriptionDetails,
    ClientUploadDetails,
)
from replit_river.error_schema import RiverException

tracer = trace.get_tracer(__name__)


class OpenTelemetryClientInterceptor(ClientInterceptor):
    async def intercept_rpc(
        self,
        details: ClientRpcDetails,
        continuation: Callable[[ClientRpcDetails], Awaitable[Any]],
    ) -> Any:
        with tracer.start_as_current_span(
            f"river.rpc.{details.service_name}.{details.procedure_name}",
            kind=trace.SpanKind.CLIENT,
        ) as span:
            try:
                return await continuation(details)
            except RiverException as e:
                span.set_attribute("river.error_code", e.code)
                span.set_attribute("river.error_message", e.message)
                return e

    async def intercept_upload(
        self,
        details: ClientUploadDetails,
        continuation: Callable[[ClientUploadDetails], Awaitable[Any]],
    ) -> Any:
        with tracer.start_as_current_span(
            f"river.upload.{details.service_name}.{details.procedure_name}",
            kind=trace.SpanKind.CLIENT,
        ) as span:
            try:
                return await continuation(details)
            except RiverException as e:
                span.set_attribute("river.error_code", e.code)
                span.set_attribute("river.error_message", e.message)
                return e

    async def intercept_subscription(
        self,
        details: ClientSubscriptionDetails,
        continuation: Callable[
            [ClientSubscriptionDetails],
            Awaitable[AsyncIterator[Any]],
        ],
    ) -> Any:
        with tracer.start_as_current_span(
            f"river.subscription.{details.service_name}.{details.procedure_name}",
            kind=trace.SpanKind.CLIENT,
        ) as span:
            try:
                return await continuation(details)
            except RiverException as e:
                span.set_attribute("river.error_code", e.code)
                span.set_attribute("river.error_message", e.message)
                return e

    async def intercept_stream(
        self,
        details: ClientStreamDetails,
        continuation: Callable[
            [ClientStreamDetails],
            Awaitable[AsyncIterator[Any]],
        ],
    ) -> Any:
        with tracer.start_as_current_span(
            f"river.stream.{details.service_name}.{details.procedure_name}",
            kind=trace.SpanKind.CLIENT,
        ) as span:
            try:
                return await continuation(details)
            except RiverException as e:
                span.set_attribute("river.error_code", e.code)
                span.set_attribute("river.error_message", e.message)
                return e
