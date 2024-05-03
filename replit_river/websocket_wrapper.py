import asyncio
import enum
import logging

from websockets import WebSocketCommonProtocol


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
        logging.error("websocket wrapper. is_open 1")
        async with self.ws_lock:
            logging.error("websocket wrapper. is_open 2")
            return self.ws_state == WsState.OPEN

    async def close(self) -> None:
        logging.error("websocket wrapper. close 1")
        async with self.ws_lock:
            logging.error("websocket wrapper. close 2")
            if self.ws_state == WsState.OPEN:
                self.ws_state = WsState.CLOSING
                task = asyncio.create_task(self.ws.close())
                task.add_done_callback(
                    lambda _: logging.debug("old websocket %s closed.", self.ws.id)
                )
                logging.error("websocket wrapper. close 3")
                self.ws_state = WsState.CLOSED
