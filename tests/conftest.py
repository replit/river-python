from typing import Any, Literal, Mapping

import nanoid
import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from replit_river.error_schema import RiverError
from replit_river.rpc import (
    GenericRpcHandler,
    TransportMessage,
)

# Modular fixtures
pytest_plugins = ["tests.river_fixtures.logging", "tests.river_fixtures.clientserver"]

HandlerKind = Literal["rpc", "subscription-stream", "upload-stream", "stream"]
HandlerMapping = Mapping[tuple[str, str], tuple[HandlerKind, GenericRpcHandler]]


def transport_message(
    seq: int = 0,
    ack: int = 0,
    streamId: str = "test_stream",
    from_: str = "client",
    to: str = "server",
    control_flag: int = 0,
    payload: Any = {},
) -> TransportMessage:
    return TransportMessage(
        id=str(nanoid.generate()),
        from_=from_,  # type: ignore
        to=to,
        streamId=streamId,
        seq=seq,
        ack=ack,
        payload=payload,
        controlFlags=control_flag,
    )


def serialize_request(request: str) -> dict:
    return {"data": request}


def deserialize_request(request: dict) -> str:
    return request.get("data") or ""


def serialize_response(response: str) -> dict:
    return {"data": response}


def deserialize_response(response: dict) -> str:
    return response.get("data") or ""


def deserialize_error(response: dict) -> RiverError:
    return RiverError.model_validate(response)


@pytest.fixture(scope="session")
def span_exporter() -> InMemorySpanExporter:
    exporter = InMemorySpanExporter()
    processor = SimpleSpanProcessor(exporter)
    provider = TracerProvider()
    provider.add_span_processor(processor)
    trace.set_tracer_provider(provider)
    return exporter


@pytest.fixture(autouse=True)
def reset_span_exporter(span_exporter: InMemorySpanExporter) -> None:
    span_exporter.clear()
