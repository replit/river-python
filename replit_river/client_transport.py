import asyncio
import logging
from typing import Optional, Tuple

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
    ControlMessageHandshakeRequest,
    ControlMessageHandshakeResponse,
    TransportMessage,
)
from replit_river.seq_manager import (
    IgnoreMessageException,
    InvalidMessageException,
)
from replit_river.session import Session
from replit_river.transport import Transport
from replit_river.transport_options import TransportOptions


class ClientTransport(Transport):
    def __init__(
        self,
        websocket_uri: str,
        client_id: str,
        server_id: str,
        transport_options: TransportOptions,
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

    async def close(self) -> None:
        self._rate_limiter.close()
        await self._close_all_sessions()

    async def _get_existing_session(self) -> Optional[ClientSession]:
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
                logging.info(f"Retrying build handshake number {i} times")
            if not rate_limit.has_budget(client_id):
                logging.debug("No retry budget for %s.", client_id)
                break
            try:
                ws = await websockets.connect(self._websocket_uri)
                existing_session = await self._get_existing_session()
                session_id = (
                    self.generate_session_id()
                    if not existing_session
                    else existing_session.session_id
                )
                logging.error(
                    f"##### _establish_new_connection: existing session : {existing_session}"
                )
                rate_limit.consume_budget(client_id)
                handshake_request, handshake_response = await self._establish_handshake(
                    self._transport_id, self._server_id, session_id, ws
                )
                rate_limit.start_restoring_budget(client_id)
                return ws, handshake_request, handshake_response
            except Exception as e:
                backoff_time = rate_limit.get_backoff_ms(client_id)
                logging.error(
                    f"Error creating session: {e}, start backoff {backoff_time} ms"
                )
                await asyncio.sleep(backoff_time / 1000)
        raise RiverException(
            ERROR_HANDSHAKE,
            "Failed to create session after retrying max number of times",
        )

    async def _create_new_session(
        self,
    ) -> ClientSession:
        new_ws, hs_request, hs_response = await self._establish_new_connection()
        advertised_session_id = hs_response.status.sessionId
        if not advertised_session_id:
            raise RiverException(
                ERROR_SESSION,
                "Server did not return a sessionId in successful handshake",
            )
        new_session = ClientSession(
            transport_id=self._transport_id,
            to_id=self._server_id,
            session_id=hs_request.sessionId,
            advertised_session_id=advertised_session_id,
            websocket=new_ws,
            transport_options=self._transport_options,
            is_server=False,
            handlers={},
            close_session_callback=self._delete_session,
            retry_connection_callback=lambda x: self._get_or_create_session(),
        )

        await self._set_session(new_session, acquire_lock=False)
        await new_session.start_serve_responses()
        return new_session

    async def _get_or_create_session(self) -> ClientSession:
        async with self._session_lock:
            existing_session = await self._get_existing_session()
            if not existing_session:
                logging.error(f"##### No existing session")
                return await self._create_new_session()
            is_session_open = await existing_session.is_session_open()
            if not is_session_open:
                logging.error(f"##### session open, creating new session")
                await existing_session.close(
                    is_unexpected_close=False, acquire_transport_lock=False
                )
                return await self._create_new_session()
            is_ws_open = existing_session.is_websocket_open()
            if is_ws_open:
                logging.error(f"##### Reuse existing session")
                return existing_session
            else:
                new_ws, _, hs_response = await self._establish_new_connection()
                if (
                    hs_response.status.sessionId
                    == existing_session.advertised_session_id
                ):
                    logging.error(f"##### session open, replacing websocket")
                    await existing_session.replace_with_new_websocket(new_ws)
                    return existing_session
                else:
                    logging.error(f"##### session open, not same session id, reuse")
                    await existing_session.close(
                        is_unexpected_close=False, acquire_transport_lock=False
                    )
                    return await self._create_new_session()

    async def _send_handshake_request(
        self,
        transport_id: str,
        to_id: str,
        session_id: str,
        websocket: WebSocketCommonProtocol,
    ) -> ControlMessageHandshakeRequest:
        handshake_request = ControlMessageHandshakeRequest(
            type="HANDSHAKE_REQ",
            protocolVersion=PROTOCOL_VERSION,
            sessionId=session_id,
        )
        stream_id = self.generate_nanoid()

        async def websocket_closed_callback() -> None:
            logging.error("websocket closed before handshake response")

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
        except (WebsocketClosedException, FailedSendingMessageException):
            raise RiverException(ERROR_HANDSHAKE, "Hand shake failed")

    async def _get_handshake_response_msg(
        self, websocket: WebSocketCommonProtocol
    ) -> TransportMessage:
        while True:
            try:
                data = await websocket.recv()
            except ConnectionClosed as e:
                logging.debug(
                    "Connection closed during waiting for handshake response : %r", e
                )
                raise RiverException(ERROR_HANDSHAKE, "Hand shake failed")
            try:
                return parse_transport_msg(data, self._transport_options)
            except IgnoreMessageException as e:
                logging.debug("Ignoring transport message : %r", e)
                continue
            except InvalidMessageException as e:
                raise RiverException(
                    ERROR_HANDSHAKE,
                    f"Got invalid transport message, closing connection : {e}",
                )

    async def _establish_handshake(
        self,
        transport_id: str,
        to_id: str,
        session_id: str,
        websocket: WebSocketCommonProtocol,
    ) -> Tuple[ControlMessageHandshakeRequest, ControlMessageHandshakeResponse]:
        try:
            handshake_request = await self._send_handshake_request(
                transport_id=transport_id,
                to_id=to_id,
                session_id=session_id,
                websocket=websocket,
            )
        except FailedSendingMessageException:
            raise RiverException(
                ERROR_CODE_STREAM_CLOSED, "Stream closed before response"
            )
        logging.debug("river client waiting for handshake response")
        try:
            response_msg = await asyncio.wait_for(
                self._get_handshake_response_msg(websocket),
                timeout=self._transport_options.session_disconnect_grace_ms / 1000,
            )
            handshake_response = ControlMessageHandshakeResponse(**response_msg.payload)
            logging.debug(
                "river client get handshake response : %r", handshake_response
            )
        except ValidationError as e:
            raise RiverException(
                ERROR_HANDSHAKE, f"Failed to parse handshake response : {e}"
            )
        except asyncio.TimeoutError:
            raise RiverException(
                ERROR_HANDSHAKE, "Handshake response timeout, closing connection"
            )
        if not handshake_response.status.ok:
            raise RiverException(
                ERROR_HANDSHAKE, f"Handshake failed: {handshake_response.status.reason}"
            )
        return handshake_request, handshake_response
