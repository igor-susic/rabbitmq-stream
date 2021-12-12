"""Module for type decodings and encodings"""
import struct
import codecs
import logging

logger = logging.getLogger(__name__)

big_endian: str = '>'
uint64: str = 'Q'
uint32: str = 'I'
uint16: str = 'H'
uint8: str = 'B'
int64: str = 'q'
int32: str = 'i'
int16: str = 'h'
int8: str = 'b'


class String:
    """
    Represents string type defined per documentation at:
        * https://github.com/rabbitmq/rabbitmq-server/blob/v3.9.x/deps/rabbitmq_stream/docs/PROTOCOL.adoc
    """
    @staticmethod
    def encode(data: str) -> bytes:
        result = codecs.encode(data, encoding='utf-8', errors='strict')
        length = len(result)
        fp = struct.pack(f'{big_endian}{uint16}', length if length != 0 else -1)

        logger.debug(f'{fp}{result}')
        return b'%b%b' % (fp, result)

    @staticmethod
    def decode(data: bytes):
        pass


class Bytes:
    pass


class Array:
    """Represents array type of binary stream protocol"""
    @staticmethod
    def encode(data: list) -> bytes:
        length = len(data)

        repeating_structure = [structure.encode() for structure in data]
        repeating_structure.insert(0, struct.pack(f'{big_endian}{uint32}', length))

        logger.debug(f'{repeating_structure}')
        return b''.join(repeating_structure)

    @staticmethod
    def decode(data: bytes):
        pass

