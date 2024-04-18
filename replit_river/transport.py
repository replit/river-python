import asyncio
import os
from typing import Dict, Tuple

import nanoid  # type: ignore  # type: ignore
from pydantic import BaseModel

from replit_river.client import PID2_PREFIX_BYTES
from replit_river.rpc import (
    GenericRpcHandler,
)
from replit_river.session import Session

PROTOCOL_VERSION = "v1"


class ConnectionRetryOptions(BaseModel):
    base_interval_ms: int = 250
    max_jitter_ms: int = 200
    max_backoff_ms: float = 32000
    attempt_budget_capacity: float = 5
    budget_restore_interval_ms: float = 200


class TransportOptions(BaseModel):
    session_disconnect_grace_ms: float = 1000
    heartbeat_ms: float = 2000
    heartbeats_until_dead: int = 2
    use_prefix_bytes: bool = False
    connection_retry_options: ConnectionRetryOptions = ConnectionRetryOptions()

    def get_prefix_bytes(self) -> bytes:
        return PID2_PREFIX_BYTES if self.use_prefix_bytes else b""

    def websocket_disconnect_grace_ms(self) -> float:
        return self.heartbeat_ms * self.heartbeats_until_dead

    def create_from_env(cls) -> "TransportOptions":
        session_disconnect_grace_ms = float(
            os.getenv("SESSION_DISCONNECT_GRACE_MS", 1000)
        )
        heartbeat_ms = float(os.getenv("HEARTBEAT_MS", 2000))
        heartbeats_to_dead = float(os.getenv("HEARTBEATS_UNTIL_DEAD", 2))
        return TransportOptions(
            session_disconnect_grace_ms=session_disconnect_grace_ms,
            heartbeat_ms=heartbeat_ms,
            heartbeats_until_dead=heartbeats_to_dead,
        )


class Transport:
    def __init__(
        self,
        transport_id: str,
        transport_options: TransportOptions,
        is_server: bool,
        handlers: Dict[Tuple[str, str], Tuple[str, GenericRpcHandler]] = {},
    ) -> None:
        self._transport_id = transport_id
        self._transport_options = transport_options
        self._is_server = is_server
        self._sessions: Dict[str, Session] = {}
        self._handlers = handlers
        self._session_lock = asyncio.Lock()

    async def on_disconnect(self, session: Session) -> None:
        await session.begin_grace()

    async def _close_session(self, session: Session) -> None:
        async with self._session_lock:
            if session._transport_id in self._sessions:
                del self._sessions[session._transport_id]

    async def generate_nanoid(self) -> str:
        return str(nanoid.generate())
