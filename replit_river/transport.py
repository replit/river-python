import asyncio
import logging
from typing import Dict, Optional, Tuple

import nanoid  # type: ignore

from replit_river.messages import FailedSendingMessageException
from replit_river.rpc import (
    GenericRpcHandler,
)
from replit_river.session import Session
from replit_river.transport_options import TransportOptions
from websockets import WebSocketCommonProtocol

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

    def generate_session_id(self) -> str:
        return self.generate_nanoid()

    async def close_all_sessions(self) -> None:
        sessions = self._sessions.values()
        logging.info(
            f"start closing transport {self._transport_id}, number sessions : "
            f"{len(sessions)}"
        )
        sessions_to_close = list(sessions)
        for session in sessions_to_close:
            await session.close(False)
        logging.info(f"Transport closed {self._transport_id}")

    async def _delete_session(self, session: Session) -> None:
        async with self._session_lock:
            if session._to_id in self._sessions:
                del self._sessions[session._to_id]

    def generate_nanoid(self) -> str:
        return str(nanoid.generate())

    async def _get_or_create_session_id(
        self,
        to_id: str,
        advertised_session_id: str,
    ):
        async with self._session_lock:
            if to_id not in self._sessions:
                return self.generate_session_id()
            else:
                old_session = self._sessions[to_id]
                if old_session._advertised_session_id != advertised_session_id:
                    return self.generate_session_id()
                else:
                    return old_session._advertised_session_id

    async def get_or_create_session(
        self,
        transport_id: str,
        to_id: str,
        session_id: str,
        advertised_session_id: str,
        websocket: WebSocketCommonProtocol,
    ) -> Session:
        session_to_close: Optional[Session] = None
        async with self._session_lock:
            if to_id not in self._sessions:
                logging.debug(
                    f'Creating new session with "{to_id}" using ws: {websocket.id}'
                )
                self._sessions[to_id] = Session(
                    transport_id,
                    to_id,
                    session_id,
                    advertised_session_id,
                    websocket,
                    self._transport_options,
                    self._is_server,
                    self._handlers,
                    close_session_callback=self._delete_session,
                )
            else:
                old_session = self._sessions[to_id]
                if old_session._advertised_session_id != advertised_session_id:
                    session_to_close = old_session
                    self._sessions[to_id] = Session(
                        transport_id,
                        to_id,
                        session_id,
                        advertised_session_id,
                        websocket,
                        self._transport_options,
                        self._is_server,
                        self._handlers,
                        close_session_callback=self._delete_session,
                    )
                else:
                    # If the instance id is the same, we reuse the session and assign
                    # a new websocket to it.
                    logging.debug(
                        f'Reuse old session with "{to_id}" using new ws: {websocket.id}'
                    )
                    try:
                        await old_session.replace_with_new_websocket(websocket)
                    except FailedSendingMessageException as e:
                        raise e
        if session_to_close:
            logging.info("Closing stale websocket")
            await session_to_close.close(False)
        session = self._sessions[to_id]
        return session
