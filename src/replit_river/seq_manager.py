import logging
from dataclasses import dataclass

from replit_river.rpc import TransportMessage

logger = logging.getLogger(__name__)


class InvalidMessageException(Exception):
    """Error processing a transport message, should raise a exception."""

    pass


class OutOfOrderMessageException(Exception):
    """Error when a message is received out of order, we close the connection
    and wait for the client to resychronize. If the resychronization fails,
    we close the session.
    """

    def __init__(self, *, received_seq: int, expected_ack: int) -> None:
        super().__init__(
            "Out of order message received: "
            f"got={received_seq}, "
            f"expected={expected_ack}"
        )


class SessionStateMismatchException(Exception):
    """Error when the session state mismatch, we reject handshake and
    close the connection"""

    pass


@dataclass
class IgnoreMessage:
    pass


class SeqManager:
    """Manages the sequence number and ack number for a connection."""

    def __init__(
        self,
    ) -> None:
        self.seq = 0
        self.ack = 0
        self.receiver_ack = 0

    def get_seq_and_increment(self) -> int:
        """Get the current sequence number and increment it.
        This removes one lock acquire than get_seq and increment_seq separately.
        """
        current_value = self.seq
        self.seq += 1
        return current_value

    def increment_seq(self) -> int:
        self.seq += 1
        return self.seq

    def get_seq(self) -> int:
        return self.seq

    def get_ack(self) -> int:
        return self.ack

    def check_seq_and_update(self, msg: TransportMessage) -> IgnoreMessage | None:
        if msg.seq != self.ack:
            if msg.seq < self.ack:
                return IgnoreMessage()
            else:
                logger.warning(
                    f"Out of order message received got {msg.seq} expected {self.ack}"
                )

                raise OutOfOrderMessageException(
                    received_seq=msg.seq,
                    expected_ack=self.ack,
                )
        self.receiver_ack = msg.ack
        self.ack = msg.seq + 1
        return None
