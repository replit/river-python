import asyncio
import logging

from replit_river.rpc import TransportMessage
from replit_river.transport_options import MAX_MESSAGE_BUFFER_SIZE

logger = logging.getLogger(__name__)


class MessageBufferClosedError(BaseException):
    """Raised when a message buffer is closed and is not accepting new messages."""


class MessageBuffer:
    """A buffer to store messages and support current updates"""

    def __init__(self, max_num_messages: int = MAX_MESSAGE_BUFFER_SIZE):
        self.max_size = max_num_messages
        self.buffer: list[TransportMessage] = []
        self._has_messages = asyncio.Event()
        self._space_available_cond = asyncio.Condition()
        self._closed = False

    async def has_capacity(self) -> None:
        async with self._space_available_cond:
            await self._space_available_cond.wait_for(
                lambda: len(self.buffer) < self.max_size or self._closed
            )

    def put(self, message: TransportMessage) -> None:
        """Add a message to the buffer. Blocks until there is space in the buffer.

        Raises:
            MessageBufferClosedError: if the buffer is closed.
        """
        if self._closed:
            raise MessageBufferClosedError("message buffer is closed")
        self.buffer.append(message)
        self._has_messages.set()

    def get_next_sent_seq(self) -> int | None:
        if self.buffer:
            return self.buffer[0].seq
        return None

    def peek(self) -> TransportMessage | None:
        """Peek the first message in the buffer, returns None if the buffer is empty."""
        if len(self.buffer) == 0:
            return None
        return self.buffer[0]

    async def remove_old_messages(self, min_seq: int) -> None:
        """Remove messages in the buffer with a seq number less than min_seq."""
        self.buffer = [msg for msg in self.buffer if msg.seq >= min_seq]
        if self.buffer:
            self._has_messages.set()
        else:
            self._has_messages.clear()
        async with self._space_available_cond:
            self._space_available_cond.notify_all()

    async def block_until_message_available(self) -> None:
        """Allow consumers to avoid spinning unnecessarily"""
        await self._has_messages.wait()

    async def close(self) -> None:
        """
        Closes the message buffer and rejects any pending put operations.
        """
        self._closed = True
        # Wake up block_until_message_available to permit graceful cleanup
        self._has_messages.set()
        async with self._space_available_cond:
            self._space_available_cond.notify_all()
