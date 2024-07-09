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
                raise InvalidMessageException("No session id in handshake request")
            try:
                return await self.get_or_create_session(
                    transport_id,
                    to_id,
                    session_id,
                    websocket,
                )
            except Exception as e:
                error_msg = (
                    "Error building sessions from handshake request : "
                    f"client_id: {transport_id}, session_id: {session_id},"
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

        oldSession = self._sessions.get(request_message.from_, None)
        if oldSession and oldSession.session_id == handshake_request.sessionId:
            # check invariants

            # ordering must be correct
            clientNextExpectedSeq = (
                handshake_request.expectedSessionState.nextExpectedSeq
            )
            clientNextSentSeq = handshake_request.expectedSessionState.nextSentSeq or 0
            ourNextSeq = await oldSession.get_next_sent_seq()
            ourAck = await oldSession.get_next_expected_seq()

            if clientNextSentSeq > ourAck:
                message = (
                    "client is in the future: "
                    f"server wanted {ourAck} but client has {clientNextSentSeq}"
                )
                await self._send_handshake_response(
                    request_message,
                    HandShakeStatus(ok=False, reason=message),
                    websocket,
                )
                raise SessionStateMismatchException(message)

            if ourNextSeq > clientNextExpectedSeq:
                message = (
                    "server is in the future: "
                    f"client wanted {clientNextExpectedSeq} but server has {ourNextSeq}"
                )
                await self._send_handshake_response(
                    request_message,
                    HandShakeStatus(ok=False, reason=message),
                    websocket,
                )
                raise SessionStateMismatchException(message)
        elif oldSession:
            # we have an old session but the session id is different
            # just delete the old session
            await oldSession.close()
            self._delete_session(oldSession)
        else:
            # no old session, see if the client is trying to resume a session
            clientNextExpectedSeq = (
                handshake_request.expectedSessionState.nextExpectedSeq
            )
            clientNextSentSeq = handshake_request.expectedSessionState.nextSentSeq or 0

            if clientNextSentSeq > 0 or clientNextExpectedSeq > 0:
                message = "client is trying to resume a session but we don't have it"
                await self._send_handshake_response(
                    request_message,
                    HandShakeStatus(ok=False, reason=message),
                    websocket,
                )
                raise SessionStateMismatchException(message)

        # from this point on, we're committed to connecting
        sessionId = handshake_request.sessionId
        handshake_response = await self._send_handshake_response(
            request_message,
            HandShakeStatus(ok=True, sessionId=sessionId),
            websocket,
        )

        return handshake_request, handshake_response
