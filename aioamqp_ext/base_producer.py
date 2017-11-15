# -*- coding: utf-8 -*-

from aioamqp_ext.base import BaseAmqp


class BaseProducer(BaseAmqp):
    NON_PERSISTENT = 1
    PERSISTENT = 2

    DEFAULT_PROPERTIES = dict(delivery_mode=PERSISTENT)

    async def _init_connection(self):
        await self.connect()
        await self.declare_exchange()

    async def publish_message(self, payload=None, routing_key=None, properties=None, mandatory=False, immediate=False):
        if properties is None:
            properties = self.DEFAULT_PROPERTIES

        if routing_key is None:
            routing_key = self._routing_key

        if not self.is_connected:
            await self._init_connection()

        await self._channel.basic_publish(
            payload=self.serialize_data(payload),
            exchange_name=self._exchange,
            routing_key=routing_key,
            properties=properties,
            mandatory=mandatory,
            immediate=immediate,
        )
