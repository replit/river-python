from typing import Any, AsyncGenerator, AsyncIterator, Iterator

import grpc
import grpc.aio

from replit_river.rpc import (
    rpc_method_handler,
    stream_method_handler,
    subscription_method_handler,
    upload_method_handler,
)
from tests.conftest import HandlerMapping, deserialize_request, serialize_response


async def rpc_handler(request: str, context: grpc.aio.ServicerContext) -> str:
    return f"Hello, {request}!"


basic_rpc_method: HandlerMapping = {
    ("test_service", "rpc_method"): (
        "rpc",
        rpc_method_handler(rpc_handler, deserialize_request, serialize_response),
    )
}


async def upload_handler(
    request: Iterator[str] | AsyncIterator[str], context: Any
) -> str:
    uploaded_data = []
    if isinstance(request, AsyncIterator):
        async for data in request:
            uploaded_data.append(data)
    else:
        for data in request:
            uploaded_data.append(data)
    return f"Uploaded: {', '.join(uploaded_data)}"


basic_upload: HandlerMapping = {
    ("test_service", "upload_method"): (
        "upload-stream",
        upload_method_handler(upload_handler, deserialize_request, serialize_response),
    ),
}


async def subscription_handler(
    request: str, context: grpc.aio.ServicerContext
) -> AsyncGenerator[str, None]:
    for i in range(5):
        yield f"Subscription message {i} for {request}"


basic_subscription: HandlerMapping = {
    ("test_service", "subscription_method"): (
        "subscription-stream",
        subscription_method_handler(
            subscription_handler, deserialize_request, serialize_response
        ),
    ),
}


async def stream_handler(
    request: Iterator[str] | AsyncIterator[str],
    context: grpc.aio.ServicerContext,
) -> AsyncGenerator[str, None]:
    if isinstance(request, AsyncIterator):
        async for data in request:
            yield f"Stream response for {data}"
    else:
        for data in request:
            yield f"Stream response for {data}"


basic_stream: HandlerMapping = {
    ("test_service", "stream_method"): (
        "stream",
        stream_method_handler(stream_handler, deserialize_request, serialize_response),
    ),
}
