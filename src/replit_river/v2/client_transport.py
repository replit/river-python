import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Generic, assert_never

import nanoid
import websockets
import websockets.asyncio.client
from pydantic import ValidationError
from websockets.asyncio.client import ClientConnection
from websockets.exceptions import ConnectionClosed

from replit_river.error_schema import (
    ERROR_CODE_STREAM_CLOSED,
    ERROR_HANDSHAKE,
    RiverException,
)
from replit_river.messages import (
    FailedSendingMessageException,
    WebsocketClosedException,
    parse_transport_msg,
    send_transport_message,
)
from replit_river.rate_limiter import LeakyBucketRateLimit
from replit_river.rpc import (
    SESSION_MISMATCH_CODE,
    ControlMessageHandshakeRequest,
    ControlMessageHandshakeResponse,
    ExpectedSessionState,
    TransportMessage,
)
from replit_river.seq_manager import (
    IgnoreMessageException,
    InvalidMessageException,
)
from replit_river.transport_options import (
    HandshakeMetadataType,
    TransportOptions,
    UriAndMetadata,
)
from replit_river.v2.session import Session

PROTOCOL_VERSION = "v2.0"

logger = logging.getLogger(__name__)


class HandshakeBudgetExhaustedException(RiverException):
    def __init__(self, code: str, message: str, client_id: str) -> None:
        super().__init__(code, message)
        self.client_id = client_id


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

    async def _close_session(self) -> None:
        logger.info(f"start closing session {self._transport_id}")
        if not self._session:
            return
        await self._session.close()
        logger.info(f"Transport closed {self._transport_id}")

    def generate_nanoid(self) -> str:
        return str(nanoid.generate())

    async def close(self) -> None:
        self._rate_limiter.close()
        await self._close_session()

    async def get_or_create_session(self) -> Session:
        """
        If we have an active session, return it.
        If we have a "closed" session, mint a whole new session.
        If we have a disconnected session, attempt to start a new WS and use it.
        """
        existing_session = self._session
        if not existing_session:
            logger.info("Creating new session")
            new_session = Session(
                transport_id=self._transport_id,
                to_id=self._server_id,
                session_id=self.generate_nanoid(),
                transport_options=self._transport_options,
                close_session_callback=self._delete_session,
                retry_connection_callback=self._retry_connection,
            )

            self._session = new_session
            existing_session = new_session
            await existing_session.start_serve_responses()

        await existing_session.ensure_connected(
            client_id=self._client_id,
            rate_limiter=self._rate_limiter,
            uri_and_metadata_factory=self._uri_and_metadata_factory,
        )
        return existing_session

    async def _establish_new_connection(
        self,
        old_session: Session | None = None,
    ) -> tuple[
        ClientConnection,
        ControlMessageHandshakeRequest[HandshakeMetadataType],
        ControlMessageHandshakeResponse,
    ]:
        """Build a new websocket connection with retry logic."""
        rate_limit = self._rate_limiter
        max_retry = self._transport_options.connection_retry_options.max_retry
        client_id = self._client_id
        logger.info("Attempting to establish new ws connection")

        last_error: Exception | None = None
        for i in range(max_retry):
            if i > 0:
                logger.info(f"Retrying build handshake number {i} times")
            if not rate_limit.has_budget(client_id):
                logger.debug("No retry budget for %s.", client_id)
                raise HandshakeBudgetExhaustedException(
                    ERROR_HANDSHAKE,
                    "No retry budget",
                    client_id=client_id,
                ) from last_error

            rate_limit.consume_budget(client_id)

            # if the session is closed, we shouldn't use it
            if old_session and not old_session.is_session_open():
                old_session = None

            try:
                uri_and_metadata = await self._uri_and_metadata_factory()
                ws = await websockets.asyncio.client.connect(uri_and_metadata["uri"])
                session_id: str
                if old_session:
                    session_id = old_session.session_id
                else:
                    session_id = self.generate_nanoid()

                try:
                    (
                        handshake_request,
                        handshake_response,
                    ) = await self._establish_handshake(
                        session_id,
                        uri_and_metadata["metadata"],
                        ws,
                        old_session,
                    )
                    rate_limit.start_restoring_budget(client_id)
                    return ws, handshake_request, handshake_response
                except RiverException as e:
                    await ws.close()
                    raise e
            except Exception as e:
                last_error = e
                backoff_time = rate_limit.get_backoff_ms(client_id)
                logger.exception(
                    f"Error connecting, retrying with {backoff_time}ms backoff"
                )
                await asyncio.sleep(backoff_time / 1000)

        raise RiverException(
            ERROR_HANDSHAKE,
            f"Failed to create ws after retrying {max_retry} number of times",
        ) from last_error

    async def _retry_connection(self) -> Session:
        if not self._transport_options.transparent_reconnect:
            await self._close_session()
        return await self.get_or_create_session()

    async def _send_handshake_request(
        self,
        session_id: str,
        handshake_metadata: HandshakeMetadataType | None,
        websocket: ClientConnection,
        expected_session_state: ExpectedSessionState,
    ) -> ControlMessageHandshakeRequest[HandshakeMetadataType]:
        handshake_request = ControlMessageHandshakeRequest[HandshakeMetadataType](
            type="HANDSHAKE_REQ",
            protocolVersion=PROTOCOL_VERSION,
            sessionId=session_id,
            metadata=handshake_metadata,
            expectedSessionState=expected_session_state,
        )
        stream_id = self.generate_nanoid()

        async def websocket_closed_callback() -> None:
            logger.error("websocket closed before handshake response")

        try:
            payload = handshake_request.model_dump()
            await send_transport_message(
                TransportMessage(
                    from_=self._transport_id,
                    to=self._server_id,
                    streamId=stream_id,
                    controlFlags=0,
                    id=self.generate_nanoid(),
                    seq=0,
                    ack=0,
                    payload=payload,
                ),
                ws=websocket,
                websocket_closed_callback=websocket_closed_callback,
            )
            return handshake_request
        except (WebsocketClosedException, FailedSendingMessageException) as e:
            raise RiverException(
                ERROR_HANDSHAKE, "Handshake failed, conn closed while sending response"
            ) from e

    async def _get_handshake_response_msg(
        self, websocket: ClientConnection
    ) -> TransportMessage:
        while True:
            try:
                data = await websocket.recv()
            except ConnectionClosed as e:
                logger.debug(
                    "Connection closed during waiting for handshake response",
                    exc_info=True,
                )
                raise RiverException(
                    ERROR_HANDSHAKE,
                    "Handshake failed, conn closed while waiting for response",
                ) from e
            try:
                msg = parse_transport_msg(data)
                if isinstance(msg, str):
                    logger.debug("Ignoring transport message", exc_info=True)
                    continue
            except InvalidMessageException as e:
                raise RiverException(
                    ERROR_HANDSHAKE,
                    "Got invalid transport message, closing connection",
                ) from e

    async def _establish_handshake(
        self,
        session_id: str,
        handshake_metadata: HandshakeMetadataType,
        websocket: ClientConnection,
        old_session: Session | None,
    ) -> tuple[
        ControlMessageHandshakeRequest[HandshakeMetadataType],
        ControlMessageHandshakeResponse,
    ]:
        try:
            expectedSessionState: ExpectedSessionState
            match old_session:
                case None:
                    expectedSessionState = ExpectedSessionState(
                        nextExpectedSeq=0,
                        nextSentSeq=0,
                    )
                case Session():
                    expectedSessionState = ExpectedSessionState(
                        nextExpectedSeq=old_session.ack,
                        nextSentSeq=old_session.seq,
                    )
                case other:
                    assert_never(other)
            handshake_request = await self._send_handshake_request(
                session_id=session_id,
                handshake_metadata=handshake_metadata,
                websocket=websocket,
                expected_session_state=expectedSessionState,
            )
        except FailedSendingMessageException as e:
            raise RiverException(
                ERROR_CODE_STREAM_CLOSED,
                "Stream closed before response, closing connection",
            ) from e

        startup_grace_sec = 60
        try:
            response_msg = await asyncio.wait_for(
                self._get_handshake_response_msg(websocket), startup_grace_sec
            )
            handshake_response = ControlMessageHandshakeResponse(**response_msg.payload)
            logger.debug("river client waiting for handshake response")
        except ValidationError as e:
            raise RiverException(
                ERROR_HANDSHAKE, "Failed to parse handshake response"
            ) from e
        except asyncio.TimeoutError as e:
            raise RiverException(
                ERROR_HANDSHAKE, "Handshake response timeout, closing connection"
            ) from e

        logger.debug("river client get handshake response : %r", handshake_response)
        if not handshake_response.status.ok:
            if old_session and handshake_response.status.code == SESSION_MISMATCH_CODE:
                # If the session status is mismatched, we should close the old session
                # and let the retry logic to create a new session.
                await old_session.close()

            raise RiverException(
                ERROR_HANDSHAKE,
                f"Handshake failed with code ${handshake_response.status.code}: "
                + f"{handshake_response.status.reason}",
            )
        return handshake_request, handshake_response

    async def _delete_session(self, session: Session) -> None:
        if self._session and session._to_id == self._session._to_id:
            self._session = None
