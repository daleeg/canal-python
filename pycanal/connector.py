# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from tornado.iostream import IOStream
import struct


class IOstreamConnector(IOStream):
    packet_len = 4

    async def read_next_packet(self):
        data = await self.read_bytes(self.packet_len)
        data_len = struct.unpack('>i', data)
        data = await self.read_bytes(data_len[0])
        return data

    async def write_with_header(self, data):
        await self.write(struct.pack('>i', len(data)))
        await self.write(data)
