import asyncio
import logging
from typing import Dict, Optional, Tuple

import nanoid  # type: ignore
from websockets import WebSocketCommonProtocol

from replit_river.messages import FailedSendingMessageException
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
        handlers: Dict[Tuple[str, str], Tuple[str, GenericRpcHandler]] = {},
    ) -> None:
        self._transport_id = transport_id
        self._transport_options = transport_options
        self._is_server = is_server
        self._sessions: Dict[str, Session] = {}
        self._handlers = handlers
        self._session_lock = asyncio.Lock()

    def generate_session_id(self) -> str:
        return self.generate_nanoid()

    async def close(self) -> None:
        await self._close_all_sessions()

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

    async def get_or_create_session(
        self,
        transport_id: str,
        to_id: str,
        session_id: str,
        websocket: WebSocketCommonProtocol,
    ) -> Session:
        async with self._session_lock:
            session_to_close: Optional[Session] = None
            new_session: Optional[Session] = None
            if to_id not in self._sessions:
                logger.debug(
                    'Creating new session with "%s" using ws: %s', to_id, websocket.id
                )
                new_session = Session(
                    transport_id,
                    to_id,
                    session_id,
                    websocket,
                    self._transport_options,
                    self._is_server,
                    self._handlers,
                    close_session_callback=self._delete_session,
                )
            else:
                old_session = self._sessions[to_id]
                if old_session.session_id != session_id:
                    logger.debug(
                        'Create new session with "%s" for session id %s'
                        " and close old session %s",
                        to_id,
                        session_id,
                        old_session.session_id,
                    )
                    session_to_close = old_session
                    new_session = Session(
                        transport_id,
                        to_id,
                        session_id,
                        websocket,
                        self._transport_options,
                        self._is_server,
                        self._handlers,
                        close_session_callback=self._delete_session,
                    )
                else:
                    # If the instance id is the same, we reuse the session and assign
                    # a new websocket to it.
                    logger.debug(
                        'Reuse old session with "%s" using new ws: %s',
                        to_id,
                        websocket.id,
                    )
                    try:
                        await old_session.replace_with_new_websocket(websocket)
                        new_session = old_session
                    except FailedSendingMessageException as e:
                        raise e

            if session_to_close:
                logger.debug("Closing stale session %s", session_to_close.session_id)
                await session_to_close.close()
            self._set_session(new_session)
        return new_session
