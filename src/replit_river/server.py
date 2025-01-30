import asyncio
import logging
from typing import Mapping, Optional, Tuple

import websockets
from websockets.exceptions import ConnectionClosed
from websockets.server import WebSocketServerProtocol

from replit_river.messages import WebsocketClosedException
from replit_river.seq_manager import SessionStateMismatchException
from replit_river.server_transport import ServerTransport
from replit_river.session import Session
from replit_river.transport import TransportOptions

from .rpc import (
    GenericRpcHandler,
)

logger = logging.getLogger(__name__)


class Server(object):
    def __init__(self, server_id: str, transport_options: TransportOptions) -> None:
        self._server_id = server_id or "SERVER"
        self._transport_options = transport_options
        self._transport = ServerTransport(
            transport_id=self._server_id,
            transport_options=transport_options,
        )

    async def close(self) -> None:
        logger.info(f"river server {self._server_id} start closing")
        await self._transport.close()
        logger.info(f"river server {self._server_id} closed")

    def add_rpc_handlers(
        self,
        rpc_handlers: Mapping[Tuple[str, str], Tuple[str, GenericRpcHandler]],
    ) -> None:
        self._transport._handlers.update(rpc_handlers)

    async def _handshake_to_get_session(
        self, websocket: WebSocketServerProtocol
    ) -> Optional[Session]:
        """This is a wrapper to make sentry happy, sentry doesn't recognize the
        exception handling outside of a task or asyncio.wait_for. So we need to catch
        the errors specifically here.
        https://docs.sentry.io/platforms/python/integrations/asyncio/#behavior
        """
        try:
            return await self._transport.handshake_to_get_session(websocket)
        except (websockets.exceptions.ConnectionClosed, WebsocketClosedException):
            # it is fine if the ws is closed during handshake, we just close the ws
            await websocket.close()
            return None
        except SessionStateMismatchException:
            logger.info("Session state mismatch, closing websocket", exc_info=True)
            await websocket.close()
            return None
        except Exception:
            logger.error(
                "Error establishing handshake, closing websocket", exc_info=True
            )
            await websocket.close()
            return None

    async def serve(self, websocket: WebSocketServerProtocol) -> None:
        logger.debug(
            "River server started establishing session with ws: %s", websocket.id
        )
        grace_ms = self._transport_options.handshake_timeout_ms
        try:
            session = await asyncio.wait_for(
                self._handshake_to_get_session(websocket),
                grace_ms / 1000,  # wait_for unit is seconds
            )
            if not session:
                return
        except asyncio.TimeoutError:
            logger.error(f"Handshake timeout after {grace_ms}ms, closing websocket")
            await websocket.close()
            return
        except asyncio.CancelledError:
            logger.error("Handshake cancelled, closing websocket")
            await websocket.close()
            return
        logger.debug("River server session established, start serving messages")

        try:
            # Session serve will be closed in two cases
            #   1. websocket is closed
            #   2. exception thrown
            # session should be kept in order to be reused by the reconnect within the
            # grace period.
            await session.serve()
        except ConnectionClosed:
            logger.debug("ConnectionClosed while serving", exc_info=True)
            # We don't have to close the websocket here, it is already closed.
        except Exception:
            logger.exception("River transport error in server %s", self._server_id)
            await websocket.close()
