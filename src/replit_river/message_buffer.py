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

    def get_next_sent_seq(self) -> int | None:
        if self.buffer:
            return self.buffer[0].seq
        return None

    def peek(self) -> TransportMessage | None:
        """Peek the first message in the buffer, returns None if the buffer is empty."""
        if len(self.buffer) == 0:
            return None
        return self.buffer[0]

    def remove_old_messages(self, min_seq: int) -> None:
        """Remove messages in the buffer with a seq number less than min_seq."""
        self.buffer = [msg for msg in self.buffer if msg.seq >= min_seq]
        async with self._space_available_cond:
            self._space_available_cond.notify_all()

    def close(self) -> None:
        """
        Closes the message buffer and rejects any pending put operations.
        """
        self._closed = True
        async with self._space_available_cond:
            self._space_available_cond.notify_all()
