# -*- coding: utf-8 -*-

import logging

import aioamqp

from aioamqp_ext.serializer import JSON, get_serializer

__all__ = ('BaseAmqp',)

DEFAULT_EXCHANGE_TYPE = 'topic'
DEFAULT_RABBIT_URL = 'amqp://localhost:5672/'
DEFAULT_PREFETCH_COUNT = 1
DEFAULT_PREFETCH_SIZE = 0

logger = logging.getLogger(__file__)


class BaseAmqp:
    def __init__(
            self,
            url=DEFAULT_RABBIT_URL,
            exchange=None,
            exchange_type=DEFAULT_EXCHANGE_TYPE,
            loop=None,
            routing_key='',
            queue=None,
            prefetch_count=DEFAULT_PREFETCH_COUNT,
            prefetch_size=DEFAULT_PREFETCH_SIZE,
            serializer=JSON
    ):
        self._url = url
        self._exchange = exchange
        self._exchange_type = exchange_type
        self._loop = loop
        self._routing_key = routing_key
        self._queue = queue
        self._prefetch_count = prefetch_count
        self._prefetch_size = prefetch_size

        self._channel = None
        self._protocol = None
        self._transport = None
        self.serializer = get_serializer(serializer)

    async def connect(self):
        self._transport, self._protocol = await aioamqp.from_url(self._url, loop=self._loop)
        self._channel = await self._protocol.channel()

    async def declare_exchange(self):
        await self._channel.exchange_declare(
            exchange_name=self._exchange,
            type_name=self._exchange_type,
            durable=True
        )

    async def declare_queue(self):
        await self._channel.queue_declare(queue_name=self._queue, durable=True)

    async def bind_queue(self):
        routing_keys_list = self._routing_key if isinstance(self._routing_key, list) else [self._routing_key]
        for routing_key in routing_keys_list:
            await self._channel.queue_bind(
                exchange_name=self._exchange,
                queue_name=self._queue,
                routing_key=routing_key
            )

    async def specify_basic_qos(self):
        await self._channel.basic_qos(
            prefetch_count=self._prefetch_count,
            prefetch_size=self._prefetch_size,
            connection_global=False
        )

    async def close(self):
        if self._protocol is not None and self._protocol.state == aioamqp.protocol.OPEN:
            await self._protocol.close()

        if self._transport is not None:
            self._transport.close()

    @property
    def is_connected(self):
        return self._protocol is not None \
               and self._protocol.state == aioamqp.protocol.OPEN \
               and self._transport is not None

    def deserialize_data(self, data):
        return self.serializer.deserialize(data)

    def serialize_data(self, data):
        return self.serializer.serialize(data)
