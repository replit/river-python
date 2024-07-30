import asyncio
import logging
from typing import Any, Optional, Tuple

import websockets
from pydantic import ValidationError
from websockets import (
    WebSocketCommonProtocol,
)
from websockets.exceptions import ConnectionClosed

from replit_river.client_session import ClientSession
from replit_river.error_schema import (
    ERROR_CODE_STREAM_CLOSED,
    ERROR_HANDSHAKE,
    ERROR_SESSION,
    RiverException,
)
from replit_river.messages import (
    PROTOCOL_VERSION,
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
from replit_river.transport import Transport
from replit_river.transport_options import TransportOptions

logger = logging.getLogger(__name__)


class ClientTransport(Transport):
    def __init__(
        self,
        websocket_uri: str,
        client_id: str,
        server_id: str,
        transport_options: TransportOptions,
        handshake_metadata: Optional[Any] = None,
    ):
        super().__init__(
            transport_id=client_id,
            transport_options=transport_options,
            is_server=False,
        )
        self._websocket_uri = websocket_uri
        self._client_id = client_id
        self._server_id = server_id
        self._rate_limiter = LeakyBucketRateLimit(
            transport_options.connection_retry_options
        )
        self._handshake_metadata = handshake_metadata
        # We want to make sure there's only one session creation at a time
        self._create_session_lock = asyncio.Lock()

    async def close(self) -> None:
        self._rate_limiter.close()
        await self._close_all_sessions()

    async def _get_existing_session(self) -> Optional[ClientSession]:
        async with self._session_lock:
            if not self._sessions:
                return None
            if len(self._sessions) > 1:
                raise RiverException(
                    "session_error",
                    "More than one session found in client, should only be one",
                )
            session = list(self._sessions.values())[0]
            if isinstance(session, ClientSession):
                return session
            else:
                raise RiverException(
                    "session_error", f"Client session type wrong, got {type(session)}"
                )

    async def _establish_new_connection(
        self,
        old_session: Optional[ClientSession] = None,
    ) -> Tuple[
        WebSocketCommonProtocol,
        ControlMessageHandshakeRequest,
        ControlMessageHandshakeResponse,
    ]:
        """Build a new websocket connection with retry logic."""
        rate_limit = self._rate_limiter
        max_retry = self._transport_options.connection_retry_options.max_retry
        client_id = self._client_id
        for i in range(max_retry):
            if i > 0:
                logger.info(f"Retrying build handshake number {i} times")
            if not rate_limit.has_budget(client_id):
                logger.debug("No retry budget for %s.", client_id)
                break
            rate_limit.consume_budget(client_id)
            try:
                ws = await websockets.connect(self._websocket_uri)
                session_id = (
                    self.generate_session_id()
                    if not old_session
                    else old_session.session_id
                )
                try:
                    (
                        handshake_request,
                        handshake_response,
                    ) = await self._establish_handshake(
                        self._transport_id,
                        self._server_id,
                        session_id,
                        self._handshake_metadata,
                        ws,
                        old_session,
                    )
                    rate_limit.start_restoring_budget(client_id)
                    return ws, handshake_request, handshake_response
                except RiverException as e:
                    await ws.close()
                    raise e
            except Exception:
                backoff_time = rate_limit.get_backoff_ms(client_id)
                logger.exception(
                    f"Error connecting, retrying with {backoff_time}ms backoff"
                )
                await asyncio.sleep(backoff_time / 1000)
        raise RiverException(
            ERROR_HANDSHAKE,
            "Failed to create ws after retrying max number of times",
        )

    async def _create_new_session(
        self,
    ) -> ClientSession:
        new_ws, hs_request, hs_response = await self._establish_new_connection()
        if not hs_response.status.ok:
            message = hs_response.status.reason
            raise RiverException(
                ERROR_SESSION,
                f"Server did not return OK status on handshake response: {message}",
            )
        new_session = ClientSession(
            transport_id=self._transport_id,
            to_id=self._server_id,
            session_id=hs_request.sessionId,
            websocket=new_ws,
            transport_options=self._transport_options,
            is_server=False,
            handlers={},
            close_session_callback=self._delete_session,
            retry_connection_callback=self._retry_connection,
        )

        self._set_session(new_session)
        await new_session.start_serve_responses()
        return new_session

    async def _retry_connection(self) -> ClientSession:
        if not self._transport_options.transparent_reconnect:
            await self._close_all_sessions()
        return await self._get_or_create_session()

    async def _get_or_create_session(self) -> ClientSession:
        async with self._create_session_lock:
            existing_session = await self._get_existing_session()
            if not existing_session:
                return await self._create_new_session()
            is_session_open = await existing_session.is_session_open()
            if not is_session_open:
                return await self._create_new_session()
            is_ws_open = await existing_session.is_websocket_open()
            if is_ws_open:
                return existing_session
            new_ws, _, hs_response = await self._establish_new_connection(
                existing_session
            )
            if hs_response.status.sessionId == existing_session.session_id:
                await existing_session.replace_with_new_websocket(new_ws)
                return existing_session
            else:
                await existing_session.close()
                return await self._create_new_session()

    async def _send_handshake_request(
        self,
        transport_id: str,
        to_id: str,
        session_id: str,
        handshake_metadata: Optional[Any],
        websocket: WebSocketCommonProtocol,
        expected_session_state: ExpectedSessionState,
    ) -> ControlMessageHandshakeRequest:
        handshake_request = ControlMessageHandshakeRequest(
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
            await send_transport_message(
                TransportMessage(
                    from_=transport_id,
                    to=to_id,
                    streamId=stream_id,
                    controlFlags=0,
                    id=self.generate_nanoid(),
                    seq=0,
                    ack=0,
                    payload=handshake_request.model_dump(),
                ),
                ws=websocket,
                prefix_bytes=self._transport_options.get_prefix_bytes(),
                websocket_closed_callback=websocket_closed_callback,
            )
            return handshake_request
        except (WebsocketClosedException, FailedSendingMessageException) as e:
            raise RiverException(
                ERROR_HANDSHAKE, "Handshake failed, conn closed while sending response"
            ) from e

    async def _get_handshake_response_msg(
        self, websocket: WebSocketCommonProtocol
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
                return parse_transport_msg(data, self._transport_options)
            except IgnoreMessageException:
                logger.debug("Ignoring transport message", exc_info=True)
                continue
            except InvalidMessageException as e:
                raise RiverException(
                    ERROR_HANDSHAKE,
                    "Got invalid transport message, closing connection",
                ) from e

    async def _establish_handshake(
        self,
        transport_id: str,
        to_id: str,
        session_id: str,
        handshake_metadata: Optional[Any],
        websocket: WebSocketCommonProtocol,
        old_session: Optional[ClientSession],
    ) -> Tuple[ControlMessageHandshakeRequest, ControlMessageHandshakeResponse]:
        try:
            handshake_request = await self._send_handshake_request(
                transport_id=transport_id,
                to_id=to_id,
                session_id=session_id,
                handshake_metadata=handshake_metadata,
                websocket=websocket,
                expected_session_state=ExpectedSessionState(
                    nextExpectedSeq=(
                        await old_session.get_next_expected_seq()
                        if old_session is not None
                        else 0
                    ),
                    nextSentSeq=(
                        await old_session.get_next_sent_seq()
                        if old_session is not None
                        else 0
                    ),
                ),
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
                await self._delete_session(old_session)

            raise RiverException(
                ERROR_HANDSHAKE, f"Handshake failed: {handshake_response.status.reason}"
            )
        return handshake_request, handshake_response
