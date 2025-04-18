import asyncio
import logging
from typing import Any

import nanoid
from pydantic import ValidationError
from websockets import (
    WebSocketCommonProtocol,
    WebSocketServerProtocol,
)
from websockets.exceptions import ConnectionClosed

from replit_river.messages import (
    FailedSendingMessageException,
    WebsocketClosedException,
    parse_transport_msg,
    send_transport_message,
)
from replit_river.rpc import (
    ControlMessageHandshakeRequest,
    ControlMessageHandshakeResponse,
    GenericRpcHandlerBuilder,
    HandShakeStatus,
    TransportMessage,
)
from replit_river.seq_manager import (
    InvalidMessageException,
    SessionStateMismatchException,
)
from replit_river.server_session import ServerSession
from replit_river.session import Session
from replit_river.transport_options import TransportOptions

PROTOCOL_VERSION = "v1.1"

logger = logging.getLogger(__name__)


class ServerTransport:
    _sessions: dict[str, ServerSession]
    _handlers: dict[tuple[str, str], tuple[str, GenericRpcHandlerBuilder]]

    def __init__(
        self,
        transport_id: str,
        transport_options: TransportOptions,
    ) -> None:
        self._sessions = {}
        self._transport_id = transport_id
        self._transport_options = transport_options
        self._handlers: dict[tuple[str, str], tuple[str, GenericRpcHandlerBuilder]] = {}
        self._session_lock = asyncio.Lock()

    async def _close_all_sessions(self) -> None:
        sessions = self._sessions.values()
        logger.info(
            f"start closing sessions {self._transport_id}, number sessions : "
            f"{len(sessions)}"
        )
        sessions_to_close = list(sessions)

        # closing sessions requires access to the session lock, so we need to close
        # them one by one to be safe
        for session in sessions_to_close:
            await session.close()

        logger.info(f"Transport closed {self._transport_id}")

    async def handshake_to_get_session(
        self,
        websocket: WebSocketServerProtocol,
    ) -> ServerSession:
        async for message in websocket:
            try:
                msg = parse_transport_msg(message)
                if isinstance(msg, str):
                    continue
                (
                    handshake_request,
                    handshake_response,
                ) = await self._establish_handshake(msg, websocket)
            except InvalidMessageException as e:
                error_msg = "Got invalid transport message, closing connection"
                raise InvalidMessageException(error_msg) from e
            except SessionStateMismatchException as e:
                raise e
            except FailedSendingMessageException as e:
                raise e
            logger.debug("handshake success on server: %r", handshake_request)
            transport_id = msg.to
            to_id = msg.from_
            session_id = handshake_response.status.sessionId
            if not session_id:
                raise InvalidMessageException("No session id in handshake request")
            try:
                return await self._get_or_create_session(
                    transport_id,
                    to_id,
                    session_id,
                    websocket,
                )
            except Exception as e:
                error_msg = (
                    "Error building sessions from handshake request : "
                    f"client_id: {transport_id}, session_id: {session_id}"
                )
                raise InvalidMessageException(error_msg) from e
        raise WebsocketClosedException("No handshake message received")

    async def close(self) -> None:
        await self._close_all_sessions()

    async def _get_or_create_session(
        self,
        transport_id: str,
        to_id: str,
        session_id: str,
        websocket: WebSocketCommonProtocol,
    ) -> ServerSession:
        new_session: ServerSession | None = None
        old_session: ServerSession | None = None
        async with self._session_lock:
            old_session = self._sessions.get(to_id)
            if not old_session:
                logger.info(
                    'Creating new session with "%s" using ws: %s', to_id, websocket.id
                )
                new_session = ServerSession(
                    transport_id,
                    to_id,
                    session_id,
                    websocket,
                    self._transport_options,
                    self._handlers,
                    close_session_callback=self._delete_session,
                )
            else:
                if old_session.session_id != session_id:
                    logger.info(
                        'Create new session with "%s" for session id %s'
                        " and close old session %s",
                        to_id,
                        session_id,
                        old_session.session_id,
                    )
                    new_session = ServerSession(
                        transport_id,
                        to_id,
                        session_id,
                        websocket,
                        self._transport_options,
                        self._handlers,
                        close_session_callback=self._delete_session,
                    )
                else:
                    # If the instance id is the same, we reuse the session and assign
                    # a new websocket to it.
                    logger.debug(
                        'Reuse old session with "%s" using new ws: %s',
                        to_id,
                        websocket.id,
                    )
                    try:
                        await old_session.replace_with_new_websocket(websocket)
                        new_session = old_session
                    except FailedSendingMessageException as e:
                        raise e

            self._sessions[new_session._to_id] = new_session

        if old_session and new_session != old_session:
            logger.info("Closing stale session %s", old_session.session_id)
            await old_session.close()

        return new_session

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
            logger.error("websocket closed before handshake response")

        try:
            await send_transport_message(
                response_message, websocket, websocket_closed_callback
            )
        except (FailedSendingMessageException, ConnectionClosed) as e:
            raise FailedSendingMessageException(
                "Failed sending handshake response"
            ) from e
        return response

    async def _establish_handshake(
        self, request_message: TransportMessage, websocket: WebSocketCommonProtocol
    ) -> tuple[
        ControlMessageHandshakeRequest[Any],
        ControlMessageHandshakeResponse,
    ]:
        try:
            handshake_request = ControlMessageHandshakeRequest[Any](
                **request_message.payload
            )
            logger.debug('Got handshake request "%r"', handshake_request)
        except (ValidationError, ValueError) as e:
            await self._send_handshake_response(
                request_message,
                HandShakeStatus(ok=False, reason="failed validate handshake request"),
                websocket,
            )
            raise InvalidMessageException("failed validate handshake request") from e

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

        async with self._session_lock:
            old_session = self._sessions.get(request_message.from_)
            client_next_expected_seq = (
                handshake_request.expectedSessionState.nextExpectedSeq
            )
            client_next_sent_seq = (
                handshake_request.expectedSessionState.nextSentSeq or 0
            )
            if old_session and old_session.session_id == handshake_request.sessionId:
                # check invariants
                # ordering must be correct
                our_next_seq = await old_session.get_next_sent_seq()
                our_ack = await old_session.get_next_expected_seq()

                if client_next_sent_seq > our_ack:
                    message = (
                        "client is in the future: "
                        f"server wanted {our_ack} but client has {client_next_sent_seq}"
                    )
                    await self._send_handshake_response(
                        request_message,
                        HandShakeStatus(ok=False, reason=message),
                        websocket,
                    )
                    raise SessionStateMismatchException(message)

                if our_next_seq > client_next_expected_seq:
                    message = (
                        "server is in the future: "
                        f"client wanted {client_next_expected_seq} "
                        f"but server has {our_next_seq}"
                    )
                    await self._send_handshake_response(
                        request_message,
                        HandShakeStatus(ok=False, reason=message),
                        websocket,
                    )
                    raise SessionStateMismatchException(message)
            elif old_session:
                old_session = None

            if not old_session and (
                client_next_sent_seq > 0 or client_next_expected_seq > 0
            ):
                message = "client is trying to resume a session but we don't have it"
                await self._send_handshake_response(
                    request_message,
                    HandShakeStatus(ok=False, reason=message),
                    websocket,
                )
                raise SessionStateMismatchException(message)

            # from this point on, we're committed to connecting
            session_id = handshake_request.sessionId
            handshake_response = await self._send_handshake_response(
                request_message,
                HandShakeStatus(ok=True, sessionId=session_id),
                websocket,
            )

            return handshake_request, handshake_response

    async def _delete_session(self, session: Session) -> None:
        async with self._session_lock:
            if session._to_id in self._sessions:
                del self._sessions[session._to_id]
