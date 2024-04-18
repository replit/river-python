import logging

from pydantic import ValidationError
from websockets import (
    WebSocketCommonProtocol,
)

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
from replit_river.transport import Transport


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
            protocolVersion="v1",
            instanceId=instance_id,
        )
        stream_id = self.generate_nanoid()
        await send_transport_message(
            TransportMessage(
                from_=transport_id,
                to=to_id,
                serviceName=None,
                procedureName=None,
                streamId=stream_id,
                controlFlags=0,
                id=0,
                seq=0,
                ack=0,
                payload=handshake_request.model_dump(),
            ),
            ws=websocket,
            prefix_bytes=self._transport_options.get_prefix_bytes(),
        )

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
        while True:
            data = await websocket.recv()
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
        except ValidationError:
            raise RiverException(ERROR_HANDSHAKE, "Failed to parse handshake response")
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
