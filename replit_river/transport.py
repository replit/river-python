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
        logging.info(
            f"start closing transport {self._transport_id}, number sessions : "
            f"{len(sessions)}"
        )
        sessions_to_close = list(sessions)
        tasks = [session.close(False) for session in sessions_to_close]
        await asyncio.gather(*tasks)
        logging.info(f"Transport closed {self._transport_id}")

    def _delete_session(self, session: Session) -> None:
        if session._to_id in self._sessions:
            del self._sessions[session._to_id]

    def _set_session(self, session: Session) -> None:
        self._sessions[session._to_id] = session

    def generate_nanoid(self) -> str:
        return str(nanoid.generate())

    async def _get_existing_session_id(
        self,
        to_id: str,
        advertised_session_id: str,
        next_expected_seq: int,
    ) -> Optional[str]:
        try:
            async with self._session_lock:
                old_session = self._sessions.get(to_id, None)
                if (
                    old_session is None
                    or await old_session.get_next_expected_ack() < next_expected_seq
                    or old_session.advertised_session_id != advertised_session_id
                ):
                    return None
                return old_session.session_id
        except Exception as e:
            logging.error(f"Error getting existing session id {e}")
            raise e

    async def _get_or_create_session_id(
        self,
        to_id: str,
        advertised_session_id: str,
    ) -> str:
        try:
            async with self._session_lock:
                if to_id not in self._sessions:
                    return self.generate_session_id()
                else:
                    old_session = self._sessions[to_id]

                    if old_session.advertised_session_id != advertised_session_id:
                        return self.generate_session_id()
                    else:
                        return old_session.session_id
        except Exception as e:
            logging.error(f"Error getting or creating session id {e}")
            raise e

    async def get_or_create_session(
        self,
        transport_id: str,
        to_id: str,
        session_id: str,
        advertised_session_id: str,
        websocket: WebSocketCommonProtocol,
    ) -> Session:
        async with self._session_lock:
            session_to_close: Optional[Session] = None
            new_session: Optional[Session] = None
            if to_id not in self._sessions:
                logging.debug(
                    'Creating new session with "%s" using ws: %s', to_id, websocket.id
                )
                new_session = Session(
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
                if old_session.advertised_session_id != advertised_session_id:
                    logging.debug(
                        'Create new session with "%s" for session id %s'
                        " and close old session %s",
                        to_id,
                        advertised_session_id,
                        old_session.advertised_session_id,
                    )
                    session_to_close = old_session
                    new_session = Session(
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
                logging.debug(
                    "Closing stale session %s", session_to_close.advertised_session_id
                )
                await session_to_close.close(is_unexpected_close=False)
            self._set_session(new_session)
        return new_session
