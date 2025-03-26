import logging
from collections.abc import Awaitable, Callable
from typing import Generic

import nanoid

from replit_river.rate_limiter import LeakyBucketRateLimit
from replit_river.transport_options import (
    HandshakeMetadataType,
    TransportOptions,
    UriAndMetadata,
)
from replit_river.v2.session import Session

logger = logging.getLogger(__name__)


class ClientTransport(Generic[HandshakeMetadataType]):
    _session: Session | None

    def __init__(
        self,
        uri_and_metadata_factory: Callable[[], Awaitable[UriAndMetadata]],
        client_id: str,
        server_id: str,
        transport_options: TransportOptions,
    ):
        self._session = None
        self._transport_id = client_id
        self._transport_options = transport_options

        self._uri_and_metadata_factory = uri_and_metadata_factory
        self._client_id = client_id
        self._server_id = server_id
        self._rate_limiter = LeakyBucketRateLimit(
            transport_options.connection_retry_options
        )

    async def close(self) -> None:
        self._rate_limiter.close()
        if self._session:
            await self._session.close()
            logger.info(f"Transport closed {self._transport_id}")

    async def get_or_create_session(self) -> Session:
        """
        Create a session if it does not exist,
        call ensure_connected on whatever session is active.
        """
        existing_session = self._session
        if not existing_session or existing_session.is_closed():
            logger.info("Creating new session")
            new_session = Session(
                transport_id=self._transport_id,
                to_id=self._server_id,
                session_id=nanoid.generate(),
                transport_options=self._transport_options,
                close_session_callback=self._delete_session,
                retry_connection_callback=self._retry_connection,
            )

            self._session = new_session
            existing_session = new_session

        await existing_session.ensure_connected(
            client_id=self._client_id,
            rate_limiter=self._rate_limiter,
            uri_and_metadata_factory=self._uri_and_metadata_factory,
        )
        return existing_session

    async def _retry_connection(self) -> Session:
        if self._session and not self._transport_options.transparent_reconnect:
            logger.info("transparent_reconnect not set, closing {self._transport_id}")
            await self._session.close()
        return await self.get_or_create_session()

    async def _delete_session(self, session: Session) -> None:
        if self._session and session._to_id == self._session._to_id:
            self._session = None
