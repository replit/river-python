import logging
from typing import Dict, Mapping, Tuple

from websockets.exceptions import ConnectionClosedError
from websockets.server import WebSocketServerProtocol

from replit_river.transport import Transport, TransportOptions

from .rpc import (
    GenericRpcHandler,
)


class Server(object):
    def __init__(self, server_id: str, transport_options: TransportOptions) -> None:
        self._handlers: Dict[Tuple[str, str], Tuple[str, GenericRpcHandler]] = {}
        self._server_id = server_id or "SERVER"
        self._transport_options = transport_options
        self._transport = Transport(
            transport_id=self._server_id,
            transport_options=transport_options,
            is_server=True,
        )

    def add_rpc_handlers(
        self,
        rpc_handlers: Mapping[Tuple[str, str], Tuple[str, GenericRpcHandler]],
    ) -> None:
        self._handlers.update(rpc_handlers)

    async def serve(self, websocket: WebSocketServerProtocol) -> None:
        logging.debug("got a client")
        try:
            session = await self._transport.establish_client_transport(websocket)
        except Exception as e:
            logging.error(f"Error establishing handshake, closing websocket: {e}")
            await websocket.close()
            return
        try:
            await session.serve()
        except ConnectionClosedError as e:
            logging.debug(f"ConnectionClosedError while serving {e}")
        except Exception as e:
            logging.error(f"River transport error in server {self._server_id}: {e}")
            await websocket.close()
