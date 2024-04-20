from typing import Any

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
