import asyncio
import logging
from asyncio import Event, shield
from collections.abc import Awaitable, Callable
from typing import Generic

import nanoid
from websockets.asyncio.client import ClientConnection

from replit_river.rate_limiter import LeakyBucketRateLimit
from replit_river.task_manager import BackgroundTaskManager
from replit_river.transport_options import (
    HandshakeMetadataType,
    TransportOptions,
    UriAndMetadata,
)
from replit_river.v2.session import Session

logger = logging.getLogger(__name__)


class ClientTransport(Generic[HandshakeMetadataType]):
    _session: Session | None
    _closing_event: tuple[Session, Event, Awaitable[None]] | None

    def __init__(
        self,
        uri_and_metadata_factory: Callable[[], Awaitable[UriAndMetadata]],
        client_id: str,
        server_id: str,
        transport_options: TransportOptions,
    ):
        self._session = None
        self._transport_id = nanoid.generate()
        self._transport_options = transport_options
        self._closing_event = None

        self._uri_and_metadata_factory = uri_and_metadata_factory
        self._client_id = client_id
        self._server_id = server_id
        self._rate_limiter = LeakyBucketRateLimit(
            transport_options.connection_retry_options
        )

    async def close(self) -> None:
        """
        A very simple function that only defers to session's close(), which
        defers to the parameter we pass in to the Session constructor.
        No logic in here.
        """
        self._rate_limiter.close()
        if self._session:
            self._session.close()

        if self._closing_event:
            await self._closing_event[1].wait()

    def _trigger_close(
        self,
        signal_closing: Callable[[], None],
        task_manager: BackgroundTaskManager,  # .cancel_all_tasks()
        terminate_remaining_output_streams: Callable[[], None],
        join_output_streams_with_timeout: Callable[[], Awaitable[None]],
        ws: ClientConnection | None,
        become_closed: Callable[[], None],
    ) -> Event:
        if self._closing_event:
            return self._closing_event[1]
        if self._session is None:
            noop = asyncio.Event()
            noop.set()
            return noop

        closing_event = Event()

        async def _do_close() -> None:
            session = self._session
            signal_closing()
            await task_manager.cancel_all_tasks()
            terminate_remaining_output_streams()
            await join_output_streams_with_timeout()
            if ws:
                await ws.close()
            become_closed()
            # Ensure that we've not established a new session in the
            # meantime somehow.
            if self._session is session:
                self._session = None
            closing_event.set()

        self._closing_event = (
            self._session,
            closing_event,
            shield(asyncio.create_task(_do_close(), name="do_close")),
        )
        return self._closing_event[1]

    async def get_or_create_session(self) -> Session:
        """
        Create a session if it does not exist,
        call ensure_connected on whatever session is active.
        """
        existing_session = self._session
        if not existing_session or existing_session.is_terminal():
            logger.info("Creating new session")
            if existing_session:
                await existing_session.close().wait()
                if self._closing_event and self._closing_event[0] == existing_session:
                    await self._closing_event[2]
                else:
                    logger.error(
                        "This should not be possible, "
                        "self._closing_event should always refer to existing_session",
                    )
            new_session = Session(
                client_id=self._client_id,
                server_id=self._server_id,
                session_id=nanoid.generate(),
                transport_options=self._transport_options,
                retry_connection_callback=self._retry_connection,
                uri_and_metadata_factory=self._uri_and_metadata_factory,
                rate_limiter=self._rate_limiter,
                trigger_close=self._trigger_close,
            )

            self._session = new_session
            existing_session = new_session

        await existing_session.ensure_connected()
        return existing_session

    async def _retry_connection(self) -> Session:
        if self._session and not self._transport_options.transparent_reconnect:
            logger.info("transparent_reconnect not set, closing {self._transport_id}")
            await self._session.close().wait()
        logger.debug("Triggering get_or_create_session")
        return await self.get_or_create_session()
