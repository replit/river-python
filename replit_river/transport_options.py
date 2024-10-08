import os
from typing import Generic, TypedDict, TypeVar

from pydantic import BaseModel

MAX_MESSAGE_BUFFER_SIZE = 128


class ConnectionRetryOptions(BaseModel):
    base_interval_ms: int = 250
    max_jitter_ms: int = 200
    max_backoff_ms: float = 32_000
    attempt_budget_capacity: float = 5
    budget_restore_interval_ms: float = 200
    max_retry: int = 5


# setup in replit web can be found at
# https://github.com/replit/repl-it-web/blob/main/pkg/pid2/src/entrypoints/protocol.ts#L13
class TransportOptions(BaseModel):
    handshake_timeout_ms: float = 10_000
    session_disconnect_grace_ms: float = 10_000
    heartbeat_ms: float = 2_500
    # TODO: This should have a better name like max_failed_heartbeats
    heartbeats_until_dead: int = 4
    close_session_check_interval_ms: float = 100
    connection_retry_options: ConnectionRetryOptions = ConnectionRetryOptions()
    buffer_size: int = 1_000
    transparent_reconnect: bool = True

    def websocket_disconnect_grace_ms(self) -> float:
        return self.heartbeat_ms * self.heartbeats_until_dead

    @classmethod
    def create_from_env(cls) -> "TransportOptions":
        handshake_timeout_ms = float(os.getenv("HANDSHAKE_TIMEOUT_MS", 5_000))
        session_disconnect_grace_ms = float(
            os.getenv("SESSION_DISCONNECT_GRACE_MS", 5_000)
        )
        heartbeat_ms = float(os.getenv("HEARTBEAT_MS", 2_000))
        heartbeats_to_dead = int(os.getenv("HEARTBEATS_UNTIL_DEAD", 2))
        return TransportOptions(
            handshake_timeout_ms=handshake_timeout_ms,
            session_disconnect_grace_ms=session_disconnect_grace_ms,
            heartbeat_ms=heartbeat_ms,
            heartbeats_until_dead=heartbeats_to_dead,
        )


HandshakeMetadataType = TypeVar("HandshakeMetadataType")


class UriAndMetadata(TypedDict, Generic[HandshakeMetadataType]):
    uri: str
    metadata: HandshakeMetadataType
