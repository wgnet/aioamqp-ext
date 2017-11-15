# -*- coding: utf-8 -*-

import logging
from abc import ABC, abstractmethod

from aioamqp_ext.base import BaseAmqp

logger = logging.getLogger(__file__)


class BaseConsumer(BaseAmqp, ABC):
    async def _init_connection(self):
        await self.connect()
        await self.declare_exchange()
        await self.declare_queue()
        await self.bind_queue()
        await self.specify_basic_qos()

    async def on_message(self, channel, body, envelope, properties):
        try:
            data = self.deserialize_data(body)
            await self.process_request(data)
        except Exception as e:
            logger.warning(e)
        finally:
            await channel.basic_client_ack(delivery_tag=envelope.delivery_tag)

    @abstractmethod
    async def process_request(self, data):
        pass

    async def consume(self):
        if not self.is_connected:
            await self._init_connection()

        await self._channel.basic_consume(self.on_message, queue_name=self._queue)
