import asyncio
import logging
from typing import Optional

import nanoid
from pydantic import ValidationError
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
            self._ws = await websockets.connect(self._websocket_uri)
            client_session = await self.create_client_session(
                self._client_id, self._server_id, self._instance_id, self._ws
            )
            self._sessions[self._server_id] = client_session
        except RiverException as e:
            logging.error(f"Error creating session: {e}")
            raise RiverException(ERROR_SESSION, "Error creating session")
        logging.debug("client start serving messages")
        await client_session.start_serve_messages()
        return client_session

    async def _get_or_create_session(self) -> ClientSession:
        async with self._session_lock:
            existing_session = await self._get_existing_session()
            if not existing_session:
                return await self._create_session()
        if not await existing_session.is_websocket_open():
            logging.debug("Client session exists but websocket closed, reconnect one")
            self._ws = await websockets.connect(self._websocket_uri)
            await existing_session.replace_with_new_websocket(self._ws)
        return existing_session

    async def _on_websocket_closed(self, session: Optional[Session]) -> None:
        if session and session.is_session_open():
            # TODO: do the retry correctly here
            logging.error("Client session websocket closed, retrying")
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

    async def create_client_session(
        self,
        transport_id: str,
        to_id: str,
        instance_id: str,
        websocket: WebSocketCommonProtocol,
    ) -> ClientSession:
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
                data = await websocket.recv()
            except ConnectionClosed as e:
                logging.error(
                    "Connection closed during waiting for handshake " f"response : {e}"
                )
                await self._on_websocket_closed(None)
                raise RiverException(ERROR_HANDSHAKE, "Hand shake failed")
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
            ack_id=first_message.id,
        )
