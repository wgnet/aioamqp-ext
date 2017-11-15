# -*- coding: utf-8 -*-

import pytest
from asynctest import CoroutineMock
from pytest_mock import MockFixture

from aioamqp_ext.base_consumer import BaseConsumer


class TestBaseConsumer:
    @staticmethod
    @pytest.fixture
    def consumer():
        BaseConsumer.__bases__ = (CoroutineMock,)

        class Consumer(BaseConsumer):
            process_request = CoroutineMock()

        return Consumer()

    @pytest.mark.asyncio
    async def test_ok_init_connection(self, consumer):
        await consumer._init_connection()

        consumer.connect.assert_called_once_with()
        consumer.declare_exchange.assert_called_once_with()
        consumer.declare_queue.assert_called_once_with()
        consumer.bind_queue.assert_called_once_with()
        consumer.specify_basic_qos.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_ok_consume_is_connected(self, consumer, mocker: MockFixture):
        mocker.patch.object(consumer, 'is_connected', True)
        mocker.patch.object(consumer, '_init_connection', CoroutineMock())

        await consumer.consume()

        fake_on_message = consumer.on_message
        fake_queue_name = consumer._queue

        consumer._init_connection.assert_not_called()
        consumer._channel.basic_consume.assert_called_once_with(
            fake_on_message,
            queue_name=fake_queue_name
        )

    @pytest.mark.asyncio
    async def test_ok_consume_is_not_connected(self, consumer, mocker: MockFixture):
        mocker.patch.object(consumer, 'is_connected', False)
        mocker.patch.object(consumer, '_init_connection', CoroutineMock())

        await consumer.consume()

        fake_on_message = consumer.on_message
        fake_queue_name = consumer._queue

        consumer._init_connection.assert_called_once_with()
        consumer._channel.basic_consume.assert_called_once_with(
            fake_on_message,
            queue_name=fake_queue_name
        )

    @pytest.mark.asyncio
    async def test_ok_on_message(self, consumer, mocker: MockFixture):
        mocker.patch.object(consumer, 'deserialize_data', mocker.Mock())

        fake_body = mocker.Mock()
        fake_channel = CoroutineMock()
        fake_envelope = mocker.Mock()
        fake_properties = dict()

        await consumer.on_message(fake_channel, fake_body, fake_envelope, fake_properties)

        consumer.deserialize_data.assert_called_once_with(fake_body)
        consumer.process_request.assert_called_once_with(consumer.deserialize_data.return_value)
        fake_channel.basic_client_ack.assert_called_once_with(delivery_tag=fake_envelope.delivery_tag)
