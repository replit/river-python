import asyncio
import logging
from typing import Optional, Tuple

import nanoid
from pydantic import ValidationError
from replit_river.rate_limiter import LeakyBucketRateLimit
from websockets import (
    WebSocketCommonProtocol,
)
import websockets
from websockets.exceptions import ConnectionClosed

from replit_river.client_session import ClientSession
from replit_river.error_schema import (
    ERROR_CODE_STREAM_CLOSED,
    ERROR_HANDSHAKE,
    ERROR_SESSION,
    RiverException,
)
from replit_river.messages import (
    FailedSendingMessageException,
    parse_transport_msg,
    send_transport_message,
)
from replit_river.rpc import (
    ACK_BIT,
    ControlMessageHandshakeRequest,
    ControlMessageHandshakeResponse,
    TransportMessage,
)
from replit_river.seq_manager import (
    IgnoreMessageException,
    InvalidMessageException,
)
from replit_river.session import Session
from replit_river.transport import PROTOCOL_VERSION, Transport
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
        self._instance_id = str(nanoid.generate())
        self._rate_limiter = LeakyBucketRateLimit(
            transport_options.connection_retry_options
        )
        # We want to make sure there's only one session creation at a time
        self._create_session_lock = asyncio.Lock()
        # Only one retry should happen at a time
        self._retry_ws_lock = asyncio.Lock()

    async def _on_session_closed(self, session: Session) -> None:
        logging.info(f"Client session {session._instance_id} closed")
        await self._delete_session(session)

    async def _retry_session_connection(self, session_to_replace_ws: Session) -> None:
        async with self._retry_ws_lock:
            if await session_to_replace_ws.is_websocket_open():
                # other retry successfully replaced the websocket,
                return
            if await session_to_replace_ws.is_session_open():
                new_ws, hs_request, hs_response = await self._establish_new_connection()
                await session_to_replace_ws.replace_with_new_websocket(new_ws)

    async def _get_existing_session(self) -> Optional[ClientSession]:
        async with self._session_lock:
            if not self._sessions:
                return None
            if len(self._sessions) > 1:
                raise RiverException(
                    "session_error",
                    "More than one session found in client, " + "should only be one",
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
        user_id = self._client_id
        for i in range(max_retry):
            if i > 0:
                logging.info(f"Retrying build handshake {i} times")
            if not rate_limit.has_budget(user_id):
                logging.debug(
                    f"No retry budget for {user_id}, waiting for budget restoration."
                )
                rate_limit.start_restoring_budget(user_id)
                await asyncio.sleep(
                    rate_limit.options.budget_restore_interval_ms / 1000.0
                )
                continue
            try:
                ws = await websockets.connect(self._websocket_uri)
                handshake_request, handshake_response = await self._establish_handshake(
                    self._transport_id, self._server_id, self._instance_id, ws
                )
                return ws, handshake_request, handshake_response
            except Exception as e:
                backoff_time = rate_limit.get_backoff_ms(user_id)
                logging.error(
                    f"Error creating session: {e}, start backoff {backoff_time} ms"
                )
                await asyncio.sleep(backoff_time / 1000)
        raise RiverException(
            ERROR_HANDSHAKE,
            "Failed to create session after retrying max number of times",
        )

    async def _get_or_create_session(self) -> ClientSession:
        async with self._create_session_lock:
            existing_session = await self._get_existing_session()
            if existing_session:
                if await existing_session.is_websocket_open():
                    return existing_session
                else:
                    await self._retry_session_connection(existing_session)
                    return existing_session
            else:
                new_ws, hs_request, hs_response = await self._establish_new_connection()
                new_session = ClientSession(
                    transport_id=self._transport_id,
                    to_id=self._server_id,
                    instance_id=self._instance_id,
                    websocket=new_ws,
                    transport_options=self._transport_options,
                    is_server=False,
                    handlers={},
                    close_session_callback=self._on_session_closed,
                    retry_connection_callback=lambda x: self._retry_session_connection(
                        x
                    ),
                )
                async with self._session_lock:
                    self._sessions[self._server_id] = new_session
                await new_session.start_serve_responses()
                return new_session

    async def _send_handshake_request(
        self,
        transport_id: str,
        to_id: str,
        instance_id: str,
        websocket: WebSocketCommonProtocol,
    ) -> ControlMessageHandshakeRequest:
        handshake_request = ControlMessageHandshakeRequest(
            type="HANDSHAKE_REQ",
            protocolVersion=PROTOCOL_VERSION,
            instanceId=instance_id,
        )
        stream_id = self.generate_nanoid()

        def websocket_closed_callback():
            raise RiverException(ERROR_SESSION, "Session closed while sending")

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
        except ConnectionClosed:
            raise RiverException(ERROR_HANDSHAKE, "Hand shake failed")

    async def _establish_handshake(
        self,
        transport_id: str,
        to_id: str,
        instance_id: str,
        websocket: WebSocketCommonProtocol,
    ) -> Tuple[ControlMessageHandshakeRequest, ControlMessageHandshakeResponse]:
        try:
            handshake_request = await self._send_handshake_request(
                transport_id=transport_id,
                to_id=to_id,
                instance_id=instance_id,
                websocket=websocket,
            )
        except FailedSendingMessageException:
            raise RiverException(
                ERROR_CODE_STREAM_CLOSED, "Stream closed before response"
            )
        logging.debug("river client waiting for handshake response")
        while True:
            try:
                logging.debug(
                    f"websocket while waiting for response : {websocket.id} {websocket.state}"
                )
                data = await websocket.recv()
            except ConnectionClosed as e:
                logging.debug(
                    "Connection closed during waiting for handshake response : {e}"
                )
                raise RiverException(ERROR_HANDSHAKE, "Hand shake failed")
            try:
                first_message = parse_transport_msg(data, self._transport_options)
            except IgnoreMessageException as e:
                logging.debug(f"Ignoring transport message : {e}")
                continue
            except InvalidMessageException as e:
                raise RiverException(
                    ERROR_HANDSHAKE,
                    f"Got invalid transport message, closing connection : {e}",
                )
            break
        try:
            handshake_response = ControlMessageHandshakeResponse(
                **first_message.payload
            )
            logging.debug(f"river client get handshake response : {handshake_response}")
        except ValidationError as e:
            raise RiverException(
                ERROR_HANDSHAKE, f"Failed to parse handshake response : {e}"
            )
        if not handshake_response.status.ok:
            raise RiverException(
                ERROR_HANDSHAKE, f"Handshake failed: {handshake_response.status.reason}"
            )
        return handshake_request, handshake_response
