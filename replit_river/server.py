import logging
from typing import Mapping, Tuple

from websockets.exceptions import ConnectionClosedError
from websockets.server import WebSocketServerProtocol

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
        await self._transport.close_all_sessions()

    def add_rpc_handlers(
        self,
        rpc_handlers: Mapping[Tuple[str, str], Tuple[str, GenericRpcHandler]],
    ) -> None:
        self._transport._handlers.update(rpc_handlers)

    async def serve(self, websocket: WebSocketServerProtocol) -> None:
        logging.debug("River server started establishing session")
        try:
            session = await self._transport.establish_session(websocket)
        except Exception as e:
            logging.error(f"Error establishing handshake, closing websocket: {e}")
            await websocket.close()
            return
        logging.debug("River server session established, start serving messages")
        try:
            await session._serve()
        except ConnectionClosedError as e:
            logging.debug(f"ConnectionClosedError while serving {e}")
        except Exception as e:
            logging.error(f"River transport error in server {self._server_id}: {e}")
            await websocket.close()
