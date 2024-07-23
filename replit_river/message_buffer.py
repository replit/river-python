import asyncio
import logging
from typing import Optional

from replit_river.rpc import TransportMessage
from replit_river.transport_options import MAX_MESSAGE_BUFFER_SIZE

logger = logging.getLogger(__name__)


class MessageBuffer:
    """A buffer to store messages and support current updates"""

    def __init__(self, max_num_messages: int = MAX_MESSAGE_BUFFER_SIZE):
        self.max_size = max_num_messages
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
                logger.error("Buffer is full, dropping message")
                raise ValueError("Buffer is full")
            self.buffer.append(message)

    async def peek(self) -> Optional[TransportMessage]:
        """Peek the first message in the buffer, returns None if the buffer is empty."""
        async with self._lock:
            if len(self.buffer) == 0:
                return None
            return self.buffer[0]

    async def remove_old_messages(self, min_seq: int) -> None:
        """Remove messages in the buffer with a seq number less than min_seq."""
        async with self._lock:
            self.buffer = [msg for msg in self.buffer if msg.seq >= min_seq]
