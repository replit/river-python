import asyncio
import logging
from collections.abc import AsyncIterable, AsyncIterator
from typing import Any, Callable, Optional, Union

import nanoid  # type: ignore
from websockets.client import WebSocketClientProtocol

from replit_river.client_session import ClientSession
from replit_river.client_transport import ClientTransport
from replit_river.error_schema import RiverException
from replit_river.task_manager import BackgroundTaskManager
from replit_river.transport_options import TransportOptions

from .rpc import (
    ErrorType,
    InitType,
    RequestType,
    ResponseType,
)


class Client:
    def __init__(
        self,
        websockets: WebSocketClientProtocol,
        client_id: str,
        server_id: str,
        transport_options: TransportOptions,
    ) -> None:
        self._ws = websockets
        self._client_id = client_id
        self._server_id = server_id
        self._instance_id = str(nanoid.generate())
        self._transport_options = transport_options
        self._transport = ClientTransport(
            transport_id=self._server_id,
            transport_options=transport_options,
            is_server=True,
        )
        self._client_session: Optional[ClientSession] = None
        self._task_manager = BackgroundTaskManager()
        asyncio.create_task(self._task_manager.create_task(self._create_session()))

    async def _create_session(self) -> None:
        try:
            logging.debug("Client start creating session")
            client_session = await self._transport.create_client_session(
                self._client_id, self._server_id, self._instance_id, self._ws
            )
        except RiverException as e:
            # TODO: we need some retry logic here.
            logging.error(f"Error creating session: {e}")
            return
        self._client_session = client_session
        logging.debug("client start serving messages")
        await self._client_session.start_serve_messages()

    async def _wait_for_handshake(self) -> ClientSession:
        while self._client_session is None:
            await asyncio.sleep(0.1)
        return self._client_session

    async def send_rpc(
        self,
        service_name: str,
        procedure_name: str,
        request: RequestType,
        request_serializer: Callable[[RequestType], Any],
        response_deserializer: Callable[[Any], ResponseType],
        error_deserializer: Callable[[Any], ErrorType],
    ) -> ResponseType:
        session = await self._wait_for_handshake()
        return await session.send_rpc(
            service_name,
            procedure_name,
            request,
            request_serializer,
            response_deserializer,
            error_deserializer,
        )

    async def send_upload(
        self,
        service_name: str,
        procedure_name: str,
        init: Optional[InitType],
        request: AsyncIterable[RequestType],
        init_serializer: Optional[Callable[[InitType], Any]],
        request_serializer: Callable[[RequestType], Any],
        response_deserializer: Callable[[Any], ResponseType],
        error_deserializer: Callable[[Any], ErrorType],
    ) -> ResponseType:
        session = await self._wait_for_handshake()
        return await session.send_upload(
            service_name,
            procedure_name,
            init,
            request,
            init_serializer,
            request_serializer,
            response_deserializer,
            error_deserializer,
        )

    async def send_subscription(
        self,
        service_name: str,
        procedure_name: str,
        request: RequestType,
        request_serializer: Callable[[RequestType], Any],
        response_deserializer: Callable[[Any], ResponseType],
        error_deserializer: Callable[[Any], ErrorType],
    ) -> AsyncIterator[Union[ResponseType, ErrorType]]:
        session = await self._wait_for_handshake()
        return session.send_subscription(
            service_name,
            procedure_name,
            request,
            request_serializer,
            response_deserializer,
            error_deserializer,
        )

    async def send_stream(
        self,
        service_name: str,
        procedure_name: str,
        init: Optional[InitType],
        request: AsyncIterable[RequestType],
        init_serializer: Optional[Callable[[InitType], Any]],
        request_serializer: Callable[[RequestType], Any],
        response_deserializer: Callable[[Any], ResponseType],
        error_deserializer: Callable[[Any], ErrorType],
    ) -> AsyncIterator[Union[ResponseType, ErrorType]]:
        session = await self._wait_for_handshake()
        return session.send_stream(
            service_name,
            procedure_name,
            init,
            request,
            init_serializer,
            request_serializer,
            response_deserializer,
            error_deserializer,
        )
