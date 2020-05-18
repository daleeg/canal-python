# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import socket
from .connector import IOstreamConnector
from tornado.ioloop import IOLoop
from .protocol import CanalProtocol_pb2
from .protocol import EntryProtocol_pb2
import logging
import platform

LOG = logging.getLogger(__name__)

if platform.system() == 'Windows':
    import asyncio

    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


class CanalClient(object):

    def __init__(self, host, port, timeout=10):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.io_loop = IOLoop.instance()
        self.get_connector()

    def get_connector(self):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(self.timeout)
            self.connector = IOstreamConnector(sock)
            self.connector.set_close_callback(self.on_close)
        except Exception as e:
            LOG.error(e)
            sock.close()
            raise Exception('stream error')

    async def connect(self):
        await self.connector.connect((self.host, self.port))
        data = await self.connector.read_next_packet()
        packet = CanalProtocol_pb2.Packet()
        packet.MergeFromString(data)
        if packet.type != CanalProtocol_pb2.PacketType.HANDSHAKE:
            raise Exception('connect error')
        print('connected to %s:%s' % (self.host, self.port))

    async def reconnect(self):
        self.disconnect()
        await self.connect()

    def disconnect(self):
        if self.connector:
            self.connector.close()
            self.connector = None

    def on_close(self):
        self.disconnect()

    async def check_valid(self, username=b'', password=b''):
        client_auth = CanalProtocol_pb2.ClientAuth()
        client_auth.username = username
        client_auth.password = password

        packet = CanalProtocol_pb2.Packet()
        packet.type = CanalProtocol_pb2.PacketType.CLIENTAUTHENTICATION
        packet.body = client_auth.SerializeToString()

        await self.connector.write_with_header(packet.SerializeToString())

        data = await self.connector.read_next_packet()
        packet = CanalProtocol_pb2.Packet()
        packet.MergeFromString(data)
        if packet.type != CanalProtocol_pb2.PacketType.ACK:
            raise Exception('Auth error')

        ack = CanalProtocol_pb2.Ack()
        ack.MergeFromString(packet.body)
        if ack.error_code > 0:
            raise Exception('something goes wrong when doing authentication. error code:%s, error message:%s' % (
                ack.error_code, ack.error_message))
        print('Auth succed')

    async def subscribe(self, client_id=b'1001', destination=b'example', filter=b'.*\\..*'):
        self.client_id = client_id
        self.destination = destination

        await self.rollback(0)

        sub = CanalProtocol_pb2.Sub()
        sub.destination = destination
        sub.client_id = client_id
        sub.filter = filter

        packet = CanalProtocol_pb2.Packet()
        packet.type = CanalProtocol_pb2.PacketType.SUBSCRIPTION
        packet.body = sub.SerializeToString()

        await self.connector.write_with_header(packet.SerializeToString())

        data = await self.connector.read_next_packet()
        packet = CanalProtocol_pb2.Packet()
        packet.MergeFromString(data)
        if packet.type != CanalProtocol_pb2.PacketType.ACK:
            raise Exception('Subscribe error.')

        ack = CanalProtocol_pb2.Ack()
        ack.MergeFromString(packet.body)
        if ack.error_code > 0:
            raise Exception(
                'Failed to subscribe. error code:%s, error message:%s' % (ack.error_code, ack.error_message))
        print('Subscribe succed')

    def unsubscribe(self):
        pass

    async def get(self, size=100):
        message = await self.get_without_ack(size)
        await self.ack(message['id'])
        return message

    async def get_without_ack(self, batch_size=10, timeout=-1, unit=-1):
        get = CanalProtocol_pb2.Get()
        get.client_id = self.client_id
        get.destination = self.destination
        get.auto_ack = False
        get.fetch_size = batch_size
        get.timeout = timeout
        get.unit = unit

        packet = CanalProtocol_pb2.Packet()
        packet.type = CanalProtocol_pb2.PacketType.GET
        packet.body = get.SerializeToString()

        await self.connector.write_with_header(packet.SerializeToString())

        data = await self.connector.read_next_packet()
        packet = CanalProtocol_pb2.Packet()
        packet.MergeFromString(data)

        message = dict(id=0, entries=[])
        if packet.type == CanalProtocol_pb2.PacketType.MESSAGES:
            messages = CanalProtocol_pb2.Messages()
            messages.MergeFromString(packet.body)
            if messages.batch_id > 0:
                message['id'] = messages.batch_id
                for item in messages.messages:
                    entry = EntryProtocol_pb2.Entry()
                    entry.MergeFromString(item)
                    message['entries'].append(entry)
        elif packet.type == CanalProtocol_pb2.PacketType.ACK:
            ack = CanalProtocol_pb2.PacketType.Ack()
            ack.MergeFromString(packet.body)
            if ack.error_code > 0:
                raise Exception('get data error. error code:%s, error message:%s' % (ack.error_code, ack.error_message))
        else:
            raise Exception('unexpected packet type:%s' % (packet.type))

        return message

    async def ack(self, message_id):
        if message_id:
            clientack = CanalProtocol_pb2.ClientAck()
            clientack.destination = self.destination
            clientack.client_id = self.client_id
            clientack.batch_id = message_id

            packet = CanalProtocol_pb2.Packet()
            packet.type = CanalProtocol_pb2.PacketType.CLIENTACK
            packet.body = clientack.SerializeToString()

            await self.connector.write_with_header(packet.SerializeToString())

    async def rollback(self, batch_id):
        cb = CanalProtocol_pb2.ClientRollback()
        cb.batch_id = batch_id
        cb.client_id = self.client_id
        cb.destination = self.destination

        packet = CanalProtocol_pb2.Packet()
        packet.type = CanalProtocol_pb2.PacketType.CLIENTROLLBACK
        packet.body = cb.SerializeToString()

        await self.connector.write_with_header(packet.SerializeToString())
