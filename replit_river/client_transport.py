import logging

from pydantic import ValidationError
from websockets import (
    WebSocketCommonProtocol,
)
from websockets.exceptions import ConnectionClosed

from replit_river.client_session import ClientSession
from replit_river.error_schema import (
    ERROR_CODE_STREAM_CLOSED,
    ERROR_HANDSHAKE,
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
from replit_river.transport import PROTOCOL_VERSION, Transport


class ClientTransport(Transport):
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
            )
        except ConnectionClosed as e:
            logging.error(f"connection closed error during handshake : {e}")

    async def close_session_callback(self, session: Session) -> None:
        pass

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
                # TODO: handle this here
                pass
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
        )
