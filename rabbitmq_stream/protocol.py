from __future__ import annotations
import struct
import logging
from dataclasses import dataclass

from typing import List

from rabbitmq_stream.types import UInt32, UInt16, Array


logger = logging.getLogger(__name__)


class Frame:
    @staticmethod
    def construct(obj) -> bytes:
        data: bytes = obj.encode()
        size: bytes = UInt32.encode(len(data))
        frame: bytes = b'%b%b' % (size, data)

        return frame

    @staticmethod
    def deconstruct(data: bytes):
        pass


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
        request_data = b'%b%b%b%b' % (
            UInt16.encode(self.key),
            UInt16.encode(self.version),
            UInt32.encode(self.correlation_id),
            Array.encode(self.peer_properties)
        )

        logger.debug(f'{request_data}')
        return request_data
