import asyncio
import enum
import logging

from websockets import WebSocketCommonProtocol

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

    async def close(self) -> None:
        async with self.ws_lock:
            if self.ws_state == WsState.OPEN:
                self.ws_state = WsState.CLOSING
                task = asyncio.create_task(self.ws.close())
                task.add_done_callback(
                    lambda _: logger.debug("old websocket %s closed.", self.ws.id)
                )
                self.ws_state = WsState.CLOSED
