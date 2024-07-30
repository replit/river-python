from typing import Any, List, Optional

from pydantic import BaseModel

ERROR_CODE_STREAM_CLOSED = "stream_closed"
ERROR_HANDSHAKE = "handshake_failed"
ERROR_SESSION = "session_error"


class RiverError(BaseModel):
    """Error message from the server."""

    code: Any
    message: str


class RiverException(Exception):
    """Exception raised by the River server."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"Error in river, code: {code}, message: {message}")


def stringify_exception(e: BaseException, limit: int = 10) -> str:
    """Return a string representation of an Exception.

    This is different from just calling str(e) because it will also show the
    chained exceptions as context.
    """
    if e.__cause__ is None:
        # If there are no causes, just fall back to stringifying the exception.
        return str(e)
    causes: List[str] = []
    cause: Optional[BaseException] = e
    while cause and limit:
        causes.append(str(cause))
        cause = cause.__cause__
        limit -= 1
    if cause:
        # If there are still causes remaining, just add an ellipsis.
        causes.append("...")
    return ": ".join(causes)
