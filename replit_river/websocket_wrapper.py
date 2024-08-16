import asyncio
import enum
import logging

from websockets import WebSocketCommonProtocol

from replit_river.task_manager import BackgroundTaskManager

logger = logging.getLogger(__name__)


class WsState(enum.Enum):
    OPEN = 0
    CLOSING = 1
    CLOSED = 2


class WebsocketWrapper:
    def __init__(self, ws: WebSocketCommonProtocol) -> None:
        self.ws = ws
        self.ws_state = WsState.OPEN
        self.ws_lock = asyncio.Lock()
        self.id = ws.id

    async def is_open(self) -> bool:
        async with self.ws_lock:
            return self.ws_state == WsState.OPEN

    async def close(self, tm: BackgroundTaskManager) -> None:
        async with self.ws_lock:
            if self.ws_state == WsState.OPEN:
                self.ws_state = WsState.CLOSING

                # Here we schedule the closing of the WebSocket into a background task,
                # because it can take an arbitrarily long time to perform (since it
                # waits for the close message). This is normally fast, but in the face
                # of network disconnects, it can take a couple of minutes for the
                # kernel to realize that the packets are being lost, and we want to
                # avoid blocking the reconnect for that to happen.
                async def _close_ws(ws: WebSocketCommonProtocol) -> None:
                    try:
                        await ws.close()
                    finally:
                        logger.debug("old websocket %s closed.", ws.id)

                tm.create_task(_close_ws(self.ws))
                self.ws_state = WsState.CLOSED
