import asyncio
import logging
from typing import Dict, Tuple

import nanoid  # type: ignore

from replit_river.rpc import (
    GenericRpcHandler,
)
from replit_river.session import Session
from replit_river.transport_options import TransportOptions

PROTOCOL_VERSION = "v1"


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

    async def close_all_sessions(self) -> None:
        sessions = self._sessions.values()
        logging.info(
            f"start closing transport {self._transport_id}, number sessions : "
            f"{len(sessions)}"
        )
        for session in sessions:
            await session.close(False)
        logging.info(f"Transport closed {self._transport_id}")

    async def _delete_session(self, session: Session) -> None:
        async with self._session_lock:
            if session._to_id in self._sessions:
                del self._sessions[session._to_id]

    def generate_nanoid(self) -> str:
        return str(nanoid.generate())
