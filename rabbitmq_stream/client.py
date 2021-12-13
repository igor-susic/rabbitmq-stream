import os
import asyncio
import typing
import logging

import rabbitmq_stream.protocol as proto

logging.basicConfig(level=logging.NOTSET)
logger = logging.getLogger(__name__)


class Connection:
    def __init__(self, host, port, ssl=None) -> None:
        self._host = host
        self._port = port
        self._ssl = ssl

        self.reader: typing.Optional[asyncio.StreamReader, None] = None
        self.writer: typing.Optional[asyncio.StreamWriter, None] = None

    async def run(self) -> None:
        await self._open_connection()
        await self._authentication_sequence()

    async def _open_connection(self) -> None:
        reader, writer = await asyncio.open_connection(host=self._host, port=self._port)
        self.reader: asyncio.StreamReader = reader
        self.writer: asyncio.StreamWriter = writer

    async def _authentication_sequence(self) -> None:
        await self._exchange_peer_properties()

    async def _exchange_peer_properties(self) -> None:
        self.writer.write(proto.Frame.construct(proto.PeerPropertiesRequest(correlation_id=1, version=1, peer_properties=[])))
        pp_response: proto.PeerPropertiesResponse = proto.PeerPropertiesResponse.decode(data=await self.reader.read())
