import pytest

import rabbitmq_stream.client as rs


@pytest.mark.asyncio
async def test_connection():
    # b'\x00\x00\x00\x0c\x00\x11\x00\x01\x00\x00\x00\x01\x00\x00\x00\x00'
    connection = rs.Connection(host="172.17.0.2", port=5552)
    await connection.run()

