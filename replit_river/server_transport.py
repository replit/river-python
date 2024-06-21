import logging
from typing import Tuple

import nanoid  # type: ignore  # type: ignore
from pydantic import ValidationError
from websockets import (
    WebSocketCommonProtocol,
    WebSocketServerProtocol,
)
from websockets.exceptions import ConnectionClosed

from replit_river.messages import (
    PROTOCOL_VERSION,
    FailedSendingMessageException,
    WebsocketClosedException,
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
    SessionStateMismatchException,
)
from replit_river.session import Session
from replit_river.transport import Transport


class ServerTransport(Transport):
    async def handshake_to_get_session(
        self,
        websocket: WebSocketServerProtocol,
    ) -> Session:
        async for message in websocket:
            try:
                msg = parse_transport_msg(message, self._transport_options)
                (
                    handshake_request,
                    handshake_response,
                ) = await self._establish_handshake(msg, websocket)
            except IgnoreMessageException:
                continue
            except InvalidMessageException:
                error_msg = "Got invalid transport message, closing connection"
                raise InvalidMessageException(error_msg)
            except SessionStateMismatchException as e:
                raise e
            except FailedSendingMessageException as e:
                raise e
            logging.debug("handshake success on server: %r", handshake_request)
            transport_id = msg.to
            to_id = msg.from_
            session_id = handshake_response.status.sessionId
            if not session_id:
                raise InvalidMessageException("No session id in handshake response")
            advertised_session_id = handshake_request.sessionId
            try:
                return await self.get_or_create_session(
                    transport_id,
                    to_id,
                    session_id,
                    advertised_session_id,
                    websocket,
                )
            except Exception as e:
                error_msg = (
                    "Error building sessions from handshake request : "
                    f"client_id: {transport_id}, session_id: {advertised_session_id},"
                    f" error: {e}"
                )
                raise InvalidMessageException(error_msg)
        raise WebsocketClosedException("No handshake message received")

    async def _send_handshake_response(
        self,
        request_message: TransportMessage,
        handshake_status: HandShakeStatus,
        websocket: WebSocketCommonProtocol,
    ) -> ControlMessageHandshakeResponse:
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
        return response

    async def _establish_handshake(
        self, request_message: TransportMessage, websocket: WebSocketCommonProtocol
    ) -> Tuple[
        ControlMessageHandshakeRequest,
        ControlMessageHandshakeResponse,
    ]:
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
        if handshake_request.expectedSessionState is None:
            # TODO: remove once we have upgraded all clients
            my_session_id = await self._get_or_create_session_id(
                request_message.from_, handshake_request.sessionId
            )
        elif handshake_request.expectedSessionState.reconnect:
            maybe_my_session_id = await self._get_existing_session_id(
                request_message.from_,
                handshake_request.sessionId,
                handshake_request.expectedSessionState.nextExpectedSeq,
            )
            if maybe_my_session_id is None:
                handshake_response = await self._send_handshake_response(
                    request_message,
                    HandShakeStatus(ok=False, reason="session state mismatch"),
                    websocket,
                )
                raise SessionStateMismatchException("session state mismatch")
            my_session_id = maybe_my_session_id
        else:
            my_session_id = self.generate_session_id()
        handshake_response = await self._send_handshake_response(
            request_message,
            HandShakeStatus(ok=True, sessionId=my_session_id),
            websocket,
        )
        return handshake_request, handshake_response
