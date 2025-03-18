import logging

from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from replit_river.session import Session

from .rpc import (
    TransportMessageTracingSetter,
)

logger = logging.getLogger(__name__)

trace_propagator = TraceContextTextMapPropagator()
trace_setter = TransportMessageTracingSetter()


class ServerSession(Session):
    """A transport object that handles the websocket connection with a client."""

    pass
