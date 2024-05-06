import asyncio
import logging
from typing import Mapping, Tuple

import websockets
from websockets.exceptions import ConnectionClosed
from websockets.server import WebSocketServerProtocol

from replit_river.messages import WebsocketClosedException
from replit_river.server_transport import ServerTransport
from replit_river.transport import TransportOptions

from .rpc import (
    GenericRpcHandler,
)


class Server(object):
    def __init__(self, server_id: str, transport_options: TransportOptions) -> None:
        self._server_id = server_id or "SERVER"
        self._transport_options = transport_options
        self._transport = ServerTransport(
            transport_id=self._server_id,
            transport_options=transport_options,
            is_server=True,
        )

    async def close(self) -> None:
        logging.info(f"river server {self._server_id} start closing")
        await self._transport.close()
        logging.info(f"river server {self._server_id} closed")

    def add_rpc_handlers(
        self,
        rpc_handlers: Mapping[Tuple[str, str], Tuple[str, GenericRpcHandler]],
    ) -> None:
        self._transport._handlers.update(rpc_handlers)

    async def serve(self, websocket: WebSocketServerProtocol) -> None:
        logging.debug(
            "River server started establishing session with ws: %s", websocket.id
        )
        try:
            session = await asyncio.wait_for(
                self._transport.handshake_to_get_session(websocket),
                self._transport_options.session_disconnect_grace_ms / 1000,
            )
        except (websockets.exceptions.ConnectionClosed, WebsocketClosedException):
            # it is fine if the ws is closed during handshake, we just close the ws
            await websocket.close()
            return
        except Exception as e:
            logging.error(
                f"Error establishing handshake, closing websocket: {e}", exc_info=True
            )
            await websocket.close()
            return
        logging.debug("River server session established, start serving messages")

        try:
            # Session serve will be closed in two cases
            #   1. websocket is closed
            #   2. exception thrown
            # session should be kept in order to be reused by the reconnect within the
            # grace period.
            await session.serve()
        except ConnectionClosed as e:
            logging.debug("ConnectionClosed while serving %r", e)
            # We don't have to close the websocket here, it is already closed.
        except Exception as e:
            logging.error(f"River transport error in server {self._server_id}: {e}")
            await websocket.close()
