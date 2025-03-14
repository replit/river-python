from typing import Any

from pydantic import BaseModel, TypeAdapter

ERROR_CODE_STREAM_CLOSED = "stream_closed"
ERROR_HANDSHAKE = "handshake_failed"
ERROR_SESSION = "session_error"


# ERROR_CODE_UNCAUGHT_ERROR is the code that is used when an error is thrown
# inside a procedure handler that's not required.
ERROR_CODE_UNCAUGHT_ERROR = "UNCAUGHT_ERROR"

# ERROR_CODE_INVALID_REQUEST is the code used when a client's request is invalid.
ERROR_CODE_INVALID_REQUEST = "INVALID_REQUEST"

# ERROR_CODE_CANCEL is the code used when either server or client cancels the stream.
ERROR_CODE_CANCEL = "CANCEL"


class RiverError(BaseModel):
    """Error message from the server."""

    code: Any
    message: str


RiverErrorTypeAdapter = TypeAdapter(RiverError)


class RiverException(Exception):
    """Exception raised by the River server."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"Error in river, code: {code}, message: {message}")


class RiverServiceException(RiverException):
    """Exception raised by river as a result of a fault in the service running river."""

    def __init__(
        self, code: str, message: str, service: str | None, procedure: str | None
    ) -> None:
        self.code = code
        self.message = message
        self.service = service
        self.procedure = procedure
        service = service or "N/A"
        procedure = procedure or "N/A"
        msg = (
            f"Error in river service ({service} - {procedure}), "
            f"code: {code}, message: {message}"
        )
        super().__init__(code, msg)


class UncaughtErrorRiverServiceException(RiverServiceException):
    pass


class InvalidRequestRiverServiceException(RiverServiceException):
    pass


class CancelRiverServiceException(RiverServiceException):
    pass


class StreamClosedRiverServiceException(RiverServiceException):
    pass


def exception_from_message(code: str) -> type[RiverServiceException]:
    """Return the error class for a given error code."""
    if code == ERROR_CODE_STREAM_CLOSED:
        return StreamClosedRiverServiceException
    elif code == ERROR_CODE_UNCAUGHT_ERROR:
        return UncaughtErrorRiverServiceException
    elif code == ERROR_CODE_INVALID_REQUEST:
        return InvalidRequestRiverServiceException
    elif code == ERROR_CODE_CANCEL:
        return CancelRiverServiceException
    return RiverServiceException


def stringify_exception(e: BaseException, limit: int = 10) -> str:
    """Return a string representation of an Exception.

    This is different from just calling str(e) because it will also show the
    chained exceptions as context.
    """
    if e.__cause__ is None:
        # If there are no causes, just fall back to stringifying the exception.
        return str(e)
    causes: list[str] = []
    cause: BaseException | None = e
    while cause and limit:
        causes.append(str(cause))
        cause = cause.__cause__
        limit -= 1
    if cause:
        # If there are still causes remaining, just add an ellipsis.
        causes.append("...")
    return ": ".join(causes)
