from abc import ABC, abstractmethod
from typing import Any, AsyncIterator, Awaitable, Callable, NamedTuple, Optional


class ClientRpcDetails(NamedTuple):
    service_name: str
    procedure_name: str
    request: Any


class ClientUploadDetails(NamedTuple):
    service_name: str
    procedure_name: str
    init: Optional[Any]


class ClientSubscriptionDetails(NamedTuple):
    service_name: str
    procedure_name: str
    request: Any


class ClientStreamDetails(NamedTuple):
    service_name: str
    procedure_name: str
    init: Optional[Any]


class ClientInterceptor(ABC):
    @abstractmethod
    async def intercept_rpc(
        self,
        details: ClientRpcDetails,
        continuation: Callable[[ClientRpcDetails], Awaitable[Any]],
    ) -> Any:
        """
        TODO: docs
        """

    @abstractmethod
    async def intercept_upload(
        self,
        details: ClientUploadDetails,
        continuation: Callable[[ClientUploadDetails], Awaitable[Any]],
    ) -> Any:
        """
        TODO: docs
        """

    @abstractmethod
    async def intercept_subscription(
        self,
        details: ClientSubscriptionDetails,
        continuation: Callable[
            [ClientSubscriptionDetails],
            Awaitable[AsyncIterator[Any]],
        ],
    ) -> AsyncIterator[Any]:
        """
        TODO: docs
        """

    @abstractmethod
    async def intercept_stream(
        self,
        details: ClientStreamDetails,
        continuation: Callable[
            [ClientStreamDetails],
            Awaitable[AsyncIterator[Any]],
        ],
    ) -> AsyncIterator[Any]:
        """
        TODO: docs
        """
