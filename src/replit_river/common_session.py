import enum
import logging
from typing import Any, Protocol

from opentelemetry.trace import Span
from websockets import WebSocketCommonProtocol
from websockets.asyncio.client import ClientConnection

logger = logging.getLogger(__name__)


class SendMessage(Protocol):
    async def __call__(
        self,
        *,
        stream_id: str,
        payload: dict[Any, Any] | str,
        control_flags: int,
        service_name: str | None,
        procedure_name: str | None,
        span: Span | None,
    ) -> None: ...


class SessionState(enum.Enum):
    """The state a session can be in.

    Valid transitions:
    - NO_CONNECTION -> {ACTIVE}
    - ACTIVE -> {NO_CONNECTION, CLOSING}
    - CLOSING -> {CLOSED}
    - CLOSED -> {}
    """

    NO_CONNECTION = 0
    ACTIVE = 1
    CLOSING = 2
    CLOSED = 3


ConnectingStateta = set([SessionState.NO_CONNECTION])
TerminalStates = set([SessionState.CLOSING, SessionState.CLOSED])
