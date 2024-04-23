import logging
from typing import Optional

import nanoid  # type: ignore  # type: ignore
from pydantic import ValidationError
from websockets import (
    WebSocketCommonProtocol,
    WebSocketServerProtocol,
)
from websockets.exceptions import ConnectionClosed
from websockets.server import WebSocketServerProtocol

from replit_river.messages import (
    FailedSendingMessageException,
    parse_transport_msg,
    send_transport_message,
)
from replit_river.rpc import (
    ControlMessageHandshakeRequest,
    ControlMessageHandshakeResponse,
    HandShakeStatus,
    TransportMessage,
)
from replit_river.seq_manager import (
    IgnoreMessageException,
    InvalidMessageException,
)
from replit_river.session import Session
from replit_river.transport import PROTOCOL_VERSION, Transport


class ServerTransport(Transport):
    async def get_or_create_session(
        self,
        transport_id: str,
        to_id: str,
        instance_id: str,
        websocket: WebSocketCommonProtocol,
    ) -> Session:
        session_to_close: Optional[Session] = None
        async with self._session_lock:
            if to_id not in self._sessions:
                logging.debug(
                    f'Creating new session with "{to_id}" using ws: {websocket.id}'
                )
                self._sessions[to_id] = Session(
                    transport_id,
                    to_id,
                    instance_id,
                    websocket,
                    self._transport_options,
                    self._is_server,
                    self._handlers,
                    close_session_callback=self._delete_session,
                )
            else:
                old_session = self._sessions[to_id]
                if old_session._instance_id != instance_id:
                    session_to_close = old_session
                    self._sessions[to_id] = Session(
                        transport_id,
                        to_id,
                        instance_id,
                        websocket,
                        self._transport_options,
                        self._is_server,
                        self._handlers,
                        close_session_callback=self._delete_session,
                    )
                else:
                    # If the instance id is the same, we reuse the session and assign
                    # a new websocket to it.
                    logging.debug(
                        f'Reuse old session with "{to_id}" using new ws: {websocket.id}'
                    )
                    try:
                        await old_session.replace_with_new_websocket(websocket)
                    except FailedSendingMessageException as e:
                        raise e
        if session_to_close:
            logging.info("Closing stale websocket")
            await session_to_close.close(False)
        session = self._sessions[to_id]
        return session

    async def handshake_to_get_session(
        self,
        websocket: WebSocketServerProtocol,
    ) -> Session:
        async for message in websocket:
            try:
                msg = parse_transport_msg(message, self._transport_options)
                handshake_request = await self._establish_handshake(msg, websocket)
            except IgnoreMessageException:
                continue
            except InvalidMessageException:
                error_msg = "Got invalid transport message, closing connection"
                raise InvalidMessageException(error_msg)
            except FailedSendingMessageException as e:
                raise e
            logging.debug("handshake success on server: %r", handshake_request)
            transport_id = msg.to
            to_id = msg.from_
            instance_id = handshake_request.instanceId
            try:
                session = await self.get_or_create_session(
                    transport_id,
                    to_id,
                    instance_id,
                    websocket,
                )
            except Exception as e:
                error_msg = (
                    "Error building sessions from handshake request : "
                    f"client_id: {transport_id}, instance_id: {instance_id}, error: {e}"
                )
                raise InvalidMessageException(error_msg)
            return session
        raise InvalidMessageException("No handshake message received")

    async def _send_handshake_response(
        self,
        request_message: TransportMessage,
        handshake_status: HandShakeStatus,
        websocket: WebSocketCommonProtocol,
    ) -> TransportMessage:
        response = ControlMessageHandshakeResponse(
            status=handshake_status,
        )
        response_message = TransportMessage(
            streamId=request_message.streamId,
            id=nanoid.generate(),
            from_=request_message.to,
            to=request_message.from_,
            seq=0,
            ack=0,
            controlFlags=0,
            payload=response.model_dump(by_alias=True, exclude_none=True),
            serviceName=request_message.serviceName,
            procedureName=request_message.procedureName,
        )

        async def websocket_closed_callback() -> None:
            logging.error("websocket closed before handshake response")

        try:
            await send_transport_message(
                response_message, websocket, websocket_closed_callback
            )
        except (FailedSendingMessageException, ConnectionClosed) as e:
            raise FailedSendingMessageException(
                f"Failed sending handshake response: {e}"
            )
        return response_message

    async def _establish_handshake(
        self, request_message: TransportMessage, websocket: WebSocketCommonProtocol
    ) -> ControlMessageHandshakeRequest:
        try:
            handshake_request = ControlMessageHandshakeRequest(
                **request_message.payload
            )
            logging.debug('Got handshake request "%r"', handshake_request)
        except (ValidationError, ValueError):
            await self._send_handshake_response(
                request_message,
                HandShakeStatus(ok=False, reason="failed validate handshake request"),
                websocket,
            )
            raise InvalidMessageException("failed validate handshake request")

        if handshake_request.protocolVersion != PROTOCOL_VERSION:
            await self._send_handshake_response(
                request_message,
                HandShakeStatus(ok=False, reason="protocol version mismatch"),
                websocket,
            )
            error_str = (
                "protocol version mismatch: "
                + f"{handshake_request.protocolVersion} != {PROTOCOL_VERSION}"
            )
            raise InvalidMessageException(error_str)
        if request_message.to != self._transport_id:
            await self._send_handshake_response(
                request_message,
                HandShakeStatus(ok=False, reason="handshake request to wrong server"),
                websocket,
            )
            raise InvalidMessageException("handshake request to wrong server")
        await self._send_handshake_response(
            request_message,
            HandShakeStatus(ok=True, instanceId=self._transport_id),
            websocket,
        )
        return handshake_request
