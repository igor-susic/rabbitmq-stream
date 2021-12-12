import os
import asyncio
import typing
import logging

import rabbitmq_stream.protocol as proto
# We should construct PeerProperties

# Frame => Size (Request | Response | Command)
#  Size => uint32 (size without the 4 bytes of the size element)

#Command => Key Version Content
# Key => uint16
# Version => uint16
# Content => bytes // see command details below

#PeerPropertiesRequest => Key Version PeerProperties
# Key => uint16 // 0x0011
# Version => uint16
# CorrelationId => uint32
# PeerProperties => [PeerProperty]
# PeerProperty => Key Value
# Key => string
# Value => string

size = '0x00000008'# uint32
key = '0x0011' # int16
version = '0x0000' # -1 in int16
content = '0x00000000'# int32 for length followed by bytes
# pp = # int32 for length followed by repetition

pp_frame = b''

# writer.write()

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

    #reader, writer = asyncio.open_connection(host=self._host, port=self._port)
    #data = b'\x00\x00\x00\x0c\x00\x11\x00\x01\x00\x00\x00\x01\x00\x00\x00\x00'
