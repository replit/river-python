import asyncio
import logging
from typing import Optional

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
    ControlMessageHandshakeRequest,
    ControlMessageHandshakeResponse,
    TransportMessage,
)
from replit_river.seq_manager import (
    IgnoreTransportMessageException,
    InvalidTransportMessageException,
)
from replit_river.session import Session
from replit_river.task_manager import BackgroundTaskManager
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

    async def _get_existing_session(self) -> Optional[ClientSession]:
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

    async def _create_session(self) -> ClientSession:
        try:
            logging.debug("Client start creating session")
            ws = await websockets.connect(self._websocket_uri)
            logging.debug(f"new ws : {ws.id} {ws.state}")
            self._ws = ws
            client_session = await self.create_client_session(
                self._client_id, self._server_id, self._instance_id, ws
            )
            self._sessions[self._server_id] = client_session
        except RiverException as e:
            logging.error(f"Error creating session: {e}")
            raise RiverException(ERROR_SESSION, "Error creating session")
        logging.debug(
            f"client start serving messages with websocket : {ws.id} {ws.state}"
        )
        await client_session.start_serve_messages()
        return client_session

    async def _get_or_create_session(self) -> ClientSession:
        existing_session = await self._get_existing_session()
        if not existing_session:
            logging.debug("Client no existing session, creating new one")
            return await self._create_session()
        if not existing_session.is_websocket_open():
            logging.debug("Client session exists but websocket closed, reconnect one")
            ws = await websockets.connect(self._websocket_uri)
            logging.debug(f"new ws : {ws.id} {ws.state}")
            self._ws = ws
            await self._send_handshake(
                self._transport_id, self._server_id, self._instance_id, ws
            )
            await existing_session.replace_with_new_websocket(ws)
        return existing_session

    async def _get_or_create_session_with_retry(self) -> ClientSession:
        async with self._session_lock:
            rate_limit = self._rate_limiter
            user_id = self._client_id
            for i in range(self._transport_options.connection_retry_options.max_retry):
                logging.info(f"Client retry build sessions {i} times")
                if rate_limit.has_budget(user_id):
                    rate_limit.consume_budget(user_id)
                    backoff_time = rate_limit.get_backoff_ms(user_id)
                    try:
                        return await self._get_or_create_session()
                    except RiverException as e:
                        logging.error(
                            f"Error creating session: {e}, start backoff {backoff_time} ms"
                        )
                        await asyncio.sleep(backoff_time / 1000)
                else:
                    logging.debug(
                        f"No budget left for {user_id}, waiting for budget restoration."
                    )
                    rate_limit.start_restoring_budget(user_id)
                    await asyncio.sleep(
                        rate_limit.options.budget_restore_interval_ms / 1000.0
                    )
            raise RiverException(
                ERROR_HANDSHAKE,
                "Failed to create session after retrying max number of times",
            )

    async def _on_websocket_closed(self, session: Optional[Session]) -> None:
        if session and session.is_session_open():
            # TODO: do the retry correctly here
            logging.error("Client session websocket closed, retrying")
            return
            self._ws = await websockets.connect(self._websocket_uri)
            await session.replace_with_new_websocket(self._ws)

    async def _send_handshake_request(
        self,
        transport_id: str,
        to_id: str,
        instance_id: str,
        websocket: WebSocketCommonProtocol,
    ) -> None:
        handshake_request = ControlMessageHandshakeRequest(
            type="HANDSHAKE_REQ",
            protocolVersion=PROTOCOL_VERSION,
            instanceId=instance_id,
        )
        stream_id = self.generate_nanoid()
        try:
            await send_transport_message(
                TransportMessage(
                    from_=transport_id,
                    to=to_id,
                    serviceName=None,
                    procedureName=None,
                    streamId=stream_id,
                    controlFlags=0,
                    id=self.generate_nanoid(),
                    seq=0,
                    ack=0,
                    payload=handshake_request.model_dump(),
                ),
                ws=websocket,
                prefix_bytes=self._transport_options.get_prefix_bytes(),
                websocket_closed_callback=lambda: self._on_websocket_closed(None),
            )
        except ConnectionClosed:
            raise RiverException(ERROR_HANDSHAKE, "Hand shake failed")

    async def close_session_callback(self, session: Session) -> None:
        logging.info(f"Client session {session._instance_id} closed")

    async def _send_handshake(
        self,
        transport_id: str,
        to_id: str,
        instance_id: str,
        websocket: WebSocketCommonProtocol,
    ) -> None:
        try:
            await self._send_handshake_request(
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
                await self._on_websocket_closed(None)
                raise RiverException(ERROR_HANDSHAKE, "Hand shake failed")

            logging.error(f"Got something")
            try:
                first_message = parse_transport_msg(data, self._transport_options)
            except IgnoreTransportMessageException as e:
                logging.debug(f"Ignoring transport message : {e}")
                continue
            except InvalidTransportMessageException as e:
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

    async def create_client_session(
        self,
        transport_id: str,
        to_id: str,
        instance_id: str,
        websocket: WebSocketCommonProtocol,
    ) -> ClientSession:
        await self._send_handshake(
            transport_id=transport_id,
            to_id=to_id,
            instance_id=instance_id,
            websocket=websocket,
        )
        return ClientSession(
            transport_id=transport_id,
            to_id=to_id,
            instance_id=instance_id,
            websocket=websocket,
            transport_options=self._transport_options,
            is_server=False,
            handlers={},
            close_session_callback=self.close_session_callback,
            close_websocket_callback=self._on_websocket_closed,
        )
