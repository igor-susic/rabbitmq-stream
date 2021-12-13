"""
Module for type decodings and encodings, defined per documentation:
* https://github.com/rabbitmq/rabbitmq-server/blob/v3.9.x/deps/rabbitmq_stream/docs/PROTOCOL.adoc
"""
import struct
import codecs
import logging

logger = logging.getLogger(__name__)

_BIG_ENDIAN: str = '>'


def _pack_num(code: str, data: int) -> bytes:
    return struct.pack(f'{_BIG_ENDIAN}{code}', data)


def _unpack_num(code: str, data: bytes) -> int:
    result: tuple = struct.unpack(f'{_BIG_ENDIAN}{code}', data)
    return result[0]


class UInt64:
    CODE: str = 'Q'

    @staticmethod
    def encode(data: int) -> bytes:
        return _pack_num(code=UInt64.CODE, data=data)

    @staticmethod
    def decode(data: bytes) -> int:
        return _unpack_num(code=UInt64.CODE, data=data)


class UInt32:
    CODE: str = 'I'

    @staticmethod
    def encode(data: int) -> bytes:
        return _pack_num(code=UInt32.CODE, data=data)

    @staticmethod
    def decode(data: bytes) -> int:
        return _unpack_num(code=UInt32.CODE, data=data)


class UInt16:
    CODE: str = 'H'

    @staticmethod
    def encode(data: int) -> bytes:
        return _pack_num(code=UInt16.CODE, data=data)

    @staticmethod
    def decode(data: bytes) -> int:
        return _unpack_num(code=UInt16.CODE, data=data)


class UInt8:
    CODE: str = 'B'

    @staticmethod
    def encode(data: int) -> bytes:
        return _pack_num(code=UInt8.CODE, data=data)

    @staticmethod
    def decode(data: bytes) -> int:
        return _unpack_num(code=UInt8.CODE, data=data)


class Int64:
    CODE: str = 'q'

    @staticmethod
    def encode(data: int) -> bytes:
        return _pack_num(code=Int64.CODE, data=data)

    @staticmethod
    def decode(data: bytes) -> int:
        return _unpack_num(code=Int64.CODE, data=data)


class Int32:
    CODE: str = 'i'

    @staticmethod
    def encode(data: int) -> bytes:
        return _pack_num(code=Int32.CODE, data=data)

    @staticmethod
    def decode(data: bytes) -> int:
        return _unpack_num(code=Int32.CODE, data=data)


class Int16:
    CODE: str = 'h'

    @staticmethod
    def encode(data: int) -> bytes:
        return _pack_num(code=Int16.CODE, data=data)

    @staticmethod
    def decode(data: bytes) -> int:
        return _unpack_num(code=Int16.CODE, data=data)


class Int8:
    CODE: str = 'b'

    @staticmethod
    def encode(data: int) -> bytes:
        return _pack_num(code=Int8.CODE, data=data)

    @staticmethod
    def decode(data: bytes) -> int:
        return _unpack_num(code=Int8.CODE, data=data)


class String:
    @staticmethod
    def encode(data: str) -> bytes:
        if data == '':
            return Int16.encode(-1)

        contents = codecs.encode(data, encoding='utf-8', errors='strict')
        size = UInt16.encode(len(contents))
        return b'%b%b' % (size, contents)

    @staticmethod
    def decode(data: bytes) -> str:
        (size, ) = struct.unpack(f'{_BIG_ENDIAN}{Int16.CODE}', data[:3])

        if not size:
            return ''

        return codecs.decode(data[3:], encoding='utf-8', errors='strict')


class Bytes:
    @staticmethod
    def encode(data: str) -> bytes:
        if data == '':
            return Int32.encode(-1)

        contents = codecs.encode(data, encoding='utf-8', errors='strict')
        size = Int32.encode(len(contents))
        return b'%b%b' % (size, contents)

    @staticmethod
    def decode(data: bytes) -> str:
        (size, ) = struct.unpack(f'{_BIG_ENDIAN}{Int32.CODE}', data[:4])

        if not size:
            return ''

        return codecs.decode(data[4:])


class Array:
    @staticmethod
    def encode(data: list) -> bytes:
        if not data:
            return UInt32.encode(0)

        repeating_structure = [structure.encode() for structure in data]
        repeating_structure.insert(0, UInt32.encode(len(data)))

        return b''.join(repeating_structure)

    @staticmethod
    def decode(data: bytes) -> list:
        (size, ) = struct.unpack(f'{_BIG_ENDIAN}{UInt32.CODE}', data[:4])

        if not size:
            return []

        return -1  # TODO decode array
