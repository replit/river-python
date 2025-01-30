import asyncio
import logging

from replit_river.rpc import TransportMessage

logger = logging.getLogger(__name__)


class IgnoreMessageException(Exception):
    """Exception to ignore a transport message, but good to continue."""

    pass


class InvalidMessageException(Exception):
    """Error processing a transport message, should raise a exception."""

    pass


class OutOfOrderMessageException(Exception):
    """Error when a message is received out of order, we close the connection
    and wait for the client to resychronize. If the resychronization fails,
    we close the session.
    """

    pass


class SessionStateMismatchException(Exception):
    """Error when the session state mismatch, we reject handshake and
    close the connection"""

    pass


class SeqManager:
    """Manages the sequence number and ack number for a connection."""

    def __init__(
        self,
    ) -> None:
        self._seq_lock = asyncio.Lock()
        self.seq = 0
        self._ack_lock = asyncio.Lock()
        self.ack = 0
        self.receiver_ack = 0

    async def get_seq_and_increment(self) -> int:
        """Get the current sequence number and increment it.
        This removes one lock acquire than get_seq and increment_seq separately.
        """
        async with self._seq_lock:
            current_value = self.seq
            self.seq += 1
            return current_value

    async def increment_seq(self) -> int:
        async with self._seq_lock:
            self.seq += 1
            return self.seq

    async def get_seq(self) -> int:
        async with self._seq_lock:
            return self.seq

    async def get_ack(self) -> int:
        async with self._ack_lock:
            return self.ack

    async def check_seq_and_update(self, msg: TransportMessage) -> None:
        async with self._ack_lock:
            if msg.seq != self.ack:
                if msg.seq < self.ack:
                    raise IgnoreMessageException(
                        f"{msg.from_} received duplicate msg, got {msg.seq}"
                        f" expected {self.ack}"
                    )
                else:
                    logger.warn(
                        f"Out of order message received got {msg.seq} expected "
                        f"{self.ack}"
                    )

                    raise OutOfOrderMessageException(
                        f"Out of order message received got {msg.seq} expected "
                        f"{self.ack}"
                    )
            self.receiver_ack = msg.ack
        await self._set_ack(msg.seq + 1)

    async def _set_ack(self, new_ack: int) -> int:
        async with self._ack_lock:
            self.ack = new_ack
            return self.ack
