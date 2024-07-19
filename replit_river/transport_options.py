import os

from pydantic import BaseModel

CROSIS_PREFIX_BYTES = b"\x00\x00"
PID2_PREFIX_BYTES = b"\xff\xff"
MAX_MESSAGE_BUFFER_SIZE = 1024


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
    session_disconnect_grace_ms: float = 10_000
    heartbeat_ms: float = 2_500
    # TODO: This should have a better name like max_failed_heartbeats
    heartbeats_until_dead: int = 4
    use_prefix_bytes: bool = False
    close_session_check_interval_ms: float = 100
    connection_retry_options: ConnectionRetryOptions = ConnectionRetryOptions()
    buffer_size: int = 1_000
    transparent_reconnect: bool = True

    def get_prefix_bytes(self) -> bytes:
        return PID2_PREFIX_BYTES if self.use_prefix_bytes else b""

    def websocket_disconnect_grace_ms(self) -> float:
        return self.heartbeat_ms * self.heartbeats_until_dead

    @classmethod
    def create_from_env(cls) -> "TransportOptions":
        session_disconnect_grace_ms = float(
            os.getenv("SESSION_DISCONNECT_GRACE_MS", 5_000)
        )
        heartbeat_ms = float(os.getenv("HEARTBEAT_MS", 2000))
        heartbeats_to_dead = int(os.getenv("HEARTBEATS_UNTIL_DEAD", 2))
        return TransportOptions(
            session_disconnect_grace_ms=session_disconnect_grace_ms,
            heartbeat_ms=heartbeat_ms,
            heartbeats_until_dead=heartbeats_to_dead,
        )
