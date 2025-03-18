import asyncio
import logging
from typing import Callable, Mapping

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
        self._handlers: dict[tuple[str, str], tuple[str, GenericRpcHandler]] = {}
        self._session_lock = asyncio.Lock()

    async def _close_all_sessions(
        self,
        get_all_sessions: Callable[[], Mapping[str, Session]],
    ) -> None:
        sessions = get_all_sessions().values()
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

    def generate_nanoid(self) -> str:
        return str(nanoid.generate())
