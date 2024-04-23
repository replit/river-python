import asyncio
import logging
from typing import Optional

from replit_river.rpc import TransportMessage


class MessageBuffer:
    """A buffer to strore messages and support current updates"""

    def __init__(self, max_size: int = 1000):
        self.max_size = max_size
        self.buffer: list[TransportMessage] = []
        self._lock = asyncio.Lock()

    async def empty(self) -> bool:
        """Check if the buffer is empty"""
        async with self._lock:
            return len(self.buffer) == 0

    async def put(self, message: TransportMessage) -> None:
        """Add a message to the buffer"""
        async with self._lock:
            if len(self.buffer) >= self.max_size:
                logging.error("Buffer is full, dropping message")
                raise ValueError("Buffer is full")
            self.buffer.append(message)

    async def peek(self) -> Optional[TransportMessage]:
        """Peek the first message in the buffer"""
        async with self._lock:
            if len(self.buffer) == 0:
                return None
            return self.buffer[0]

    async def remove_old_messages(self, ack: int) -> None:
        """Remove all messages in the buffer"""
        async with self._lock:
            self.buffer = [msg for msg in self.buffer if msg.seq >= ack]
