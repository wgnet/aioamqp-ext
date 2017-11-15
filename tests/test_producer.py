# -*- coding: utf-8 -*-

import pytest
from asynctest import CoroutineMock
from pytest_mock import MockFixture

from aioamqp_ext.base_producer import BaseProducer


@pytest.fixture
def producer():
    BaseProducer.__bases__ = (CoroutineMock,)

    return BaseProducer()


class TestBaseProducerInitConnection:
    @pytest.mark.asyncio
    async def test_ok(self, producer):
        await producer._init_connection()

        producer.connect.assert_called_once_with()
        producer.declare_exchange.assert_called_once_with()


class TestBaseProducerPublishMessage:
    @staticmethod
    @pytest.fixture
    def patched_producer(producer, mocker: MockFixture):
        mocker.patch.object(producer, '_init_connection', CoroutineMock())
        mocker.patch.object(producer, 'serialize_data', mocker.Mock())

        return producer

    @pytest.mark.asyncio
    async def test_ok_is_connected_with_payload(self, patched_producer, mocker: MockFixture):
        mocker.patch.object(patched_producer, 'is_connected', True)

        fake_payload = mocker.Mock()

        await patched_producer.publish_message(payload=fake_payload)

        patched_producer._init_connection.assert_not_called()
        patched_producer.serialize_data.assert_called_once_with(fake_payload)
        patched_producer._channel.basic_publish.assert_called_once_with(
            payload=patched_producer.serialize_data.return_value,
            exchange_name=patched_producer._exchange,
            routing_key=patched_producer._routing_key,
            properties=BaseProducer.DEFAULT_PROPERTIES,
            mandatory=False,
            immediate=False,
        )

    @pytest.mark.asyncio
    async def test_ok_is_connected_wo_payload_with_routing_key(self, patched_producer, mocker: MockFixture):
        mocker.patch.object(patched_producer, 'is_connected', True)

        fake_routing_key = mocker.Mock()

        await patched_producer.publish_message(routing_key=fake_routing_key)

        patched_producer._init_connection.assert_not_called()
        patched_producer.serialize_data.assert_called_once_with(None)
        patched_producer._channel.basic_publish.assert_called_once_with(
            payload=patched_producer.serialize_data.return_value,
            exchange_name=patched_producer._exchange,
            routing_key=fake_routing_key,
            properties=BaseProducer.DEFAULT_PROPERTIES,
            mandatory=False,
            immediate=False,
        )

    @pytest.mark.asyncio
    async def test_ok_is_connected_wo_payload_with_properties(self, patched_producer, mocker: MockFixture):
        mocker.patch.object(patched_producer, 'is_connected', True)

        fake_properties = mocker.Mock()

        await patched_producer.publish_message(properties=fake_properties)

        patched_producer._init_connection.assert_not_called()
        patched_producer.serialize_data.assert_called_once_with(None)
        patched_producer._channel.basic_publish.assert_called_once_with(
            payload=patched_producer.serialize_data.return_value,
            exchange_name=patched_producer._exchange,
            routing_key=patched_producer._routing_key,
            properties=fake_properties,
            mandatory=False,
            immediate=False,
        )

    @pytest.mark.asyncio
    async def test_ok_is_not_connected_with_payload(self, patched_producer, mocker: MockFixture):
        mocker.patch.object(patched_producer, 'is_connected', False)

        fake_payload = mocker.Mock()

        await patched_producer.publish_message(payload=fake_payload)

        patched_producer._init_connection.assert_called_once_with()
        patched_producer.serialize_data.assert_called_once_with(fake_payload)
        patched_producer._channel.basic_publish.assert_called_once_with(
            payload=patched_producer.serialize_data.return_value,
            exchange_name=patched_producer._exchange,
            routing_key=patched_producer._routing_key,
            properties=BaseProducer.DEFAULT_PROPERTIES,
            mandatory=False,
            immediate=False,
        )

    @pytest.mark.asyncio
    async def test_ok_with_extra_params(self, patched_producer, mocker: MockFixture):
        mocker.patch.object(patched_producer, 'is_connected', True)

        fake_payload = mocker.Mock()
        fake_mandatory = mocker.Mock()
        fake_immediate = mocker.Mock()

        await patched_producer.publish_message(payload=fake_payload, mandatory=fake_mandatory, immediate=fake_immediate)

        patched_producer._init_connection.assert_not_called()
        patched_producer.serialize_data.assert_called_once_with(fake_payload)
        patched_producer._channel.basic_publish.assert_called_once_with(
            payload=patched_producer.serialize_data.return_value,
            exchange_name=patched_producer._exchange,
            routing_key=patched_producer._routing_key,
            properties=BaseProducer.DEFAULT_PROPERTIES,
            mandatory=fake_mandatory,
            immediate=fake_immediate,
        )
