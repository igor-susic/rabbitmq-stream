from __future__ import annotations
import struct
import logging
from dataclasses import dataclass

from typing import List

import rabbitmq_stream.codecs as codecs


logger = logging.getLogger(__name__)


class Frame:
    @staticmethod
    def construct(obj) -> bytes:
        data: bytes = obj.encode()
        data_length: int = len(data)
        dl = struct.pack(f'{codecs.big_endian}{codecs.uint32}', data_length)
        logger.debug(f'Data length  = {data_length}, decoded to: {dl}')
        frame: bytes = b'%b%b' % (dl, data)
        logger.debug('Sending frame: %s', frame)
        return frame

    @staticmethod
    def deconstruct(data):
        pass



# class NetworkReader:
#     pass

# def Unpacker:
#     def uint32(self, bytes) -> int:
#         return struct.unpack('I', bytes)
#
#     def uint16(self, bytes) -> int:
#         return struct.unpack


@dataclass
class PeerProperty:
    key: str
    value: str


@dataclass
class PeerPropertiesResponse:
    key: int
    version: int
    correlation_id: int
    response_code: int
    peer_properties: List[PeerProperty]

    @staticmethod
    def decode(data: bytes) -> PeerPropertiesResponse:
        logger.debug(f'{data}')
        pass


@dataclass
class PeerPropertiesRequest:
    version: int
    correlation_id: int
    peer_properties: List[PeerProperty]
    key: int = 17

    def encode(self) -> bytes:
        format_string = f'{codecs.big_endian}{codecs.uint16*2}{codecs.uint32}'
        first_part_of_structure = struct.pack(format_string, self.key, self.version, self.correlation_id)
        second_part_of_structure = codecs.Array.encode(self.peer_properties)
        request_data = b'%b%b' % (first_part_of_structure, second_part_of_structure)

        logger.debug(f'{request_data}')
        return request_data
