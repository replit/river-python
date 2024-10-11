import asyncio
import logging
from typing import Dict, Tuple

import nanoid  # type: ignore

from replit_river.rpc import (
    GenericRpcHandler,
)
from replit_river.session import Session
from replit_river.transport_options import TransportOptions

logger = logging.getLogger(__name__)


class Transport:
    def __init__(
        self,
        transport_id: str,
        transport_options: TransportOptions,
        is_server: bool,
    ) -> None:
        self._transport_id = transport_id
        self._transport_options = transport_options
        self._is_server = is_server
        self._sessions: Dict[str, Session] = {}
        self._handlers: Dict[Tuple[str, str], Tuple[str, GenericRpcHandler]] = {}
        self._session_lock = asyncio.Lock()

    async def _close_all_sessions(self) -> None:
        sessions = self._sessions.values()
        logger.info(
            f"start closing sessions {self._transport_id}, number sessions : "
            f"{len(sessions)}"
        )
        sessions_to_close = list(sessions)

        # closing sessions requires access to the session lock, so we need to close
        # them one by one to be safe
        for session in sessions_to_close:
            await session.close()

        logger.info(f"Transport closed {self._transport_id}")

    async def _delete_session(self, session: Session) -> None:
        async with self._session_lock:
            if session._to_id in self._sessions:
                del self._sessions[session._to_id]

    def _set_session(self, session: Session) -> None:
        self._sessions[session._to_id] = session

    def generate_nanoid(self) -> str:
        return str(nanoid.generate())
