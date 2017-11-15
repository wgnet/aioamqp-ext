# -*- coding: utf-8 -*-

import pytest
from asynctest import CoroutineMock
from pytest_mock import MockFixture

from aioamqp_ext.base import BaseAmqp
from aioamqp_ext.serializer import JsonSerializer, MsgPackSerializer, JSON, MSGPACK


@pytest.fixture
def amqp():
    return BaseAmqp()


class TestBaseAmqpInit:
    @staticmethod
    def assert_amqp_attrs(amqp_obj):
        assert hasattr(amqp_obj, '_url')
        assert hasattr(amqp_obj, '_exchange')
        assert hasattr(amqp_obj, '_exchange_type')
        assert hasattr(amqp_obj, '_loop')
        assert hasattr(amqp_obj, '_routing_key')
        assert hasattr(amqp_obj, '_queue')
        assert hasattr(amqp_obj, '_prefetch_count')
        assert hasattr(amqp_obj, '_prefetch_size')
        assert hasattr(amqp_obj, '_channel')
        assert hasattr(amqp_obj, '_protocol')
        assert hasattr(amqp_obj, '_transport')
        assert hasattr(amqp_obj, 'serializer')

    def test_ok_default_serializer(self, mocker: MockFixture):
        mocked_serializer = mocker.patch(
            'aioamqp_ext.base.get_serializer',
            return_value=mocker.Mock(spec=JsonSerializer)
        )
        amqp_obj = BaseAmqp()

        mocked_serializer.assert_called_once_with(JSON)
        self.assert_amqp_attrs(amqp_obj)

        assert isinstance(amqp_obj.serializer, JsonSerializer)

    @pytest.mark.parametrize('encoding, serializer', [
        (JSON, JsonSerializer),
        (MSGPACK, MsgPackSerializer)
    ])
    def test_ok_serializer(self, encoding, serializer, mocker: MockFixture):
        mocked_serializer = mocker.patch(
            'aioamqp_ext.base.get_serializer',
            return_value=mocker.Mock(spec=serializer)
        )
        amqp_obj = BaseAmqp(serializer=encoding)

        mocked_serializer.assert_called_once_with(encoding)
        self.assert_amqp_attrs(amqp_obj)

        assert isinstance(amqp_obj.serializer, serializer)

    def test_error_unknown_serializer(self):
        with pytest.raises(LookupError):
            BaseAmqp(serializer='unknown')


class TestBaseAmqpConnect:
    @pytest.mark.asyncio
    async def test_ok(self, amqp: BaseAmqp, mocker: MockFixture):
        fake_transport = mocker.Mock()
        fake_protocol = CoroutineMock()
        mocked_from_url = mocker.patch(
            'aioamqp.from_url',
            CoroutineMock(return_value=(fake_transport, fake_protocol))
        )

        await amqp.connect()

        mocked_from_url.assert_called_once_with(amqp._url, loop=amqp._loop)
        fake_protocol.channel.assert_called_once_with()

        assert amqp._transport == fake_transport
        assert amqp._protocol == fake_protocol


class TestBaseAmqpBasic:
    @staticmethod
    @pytest.fixture
    def patched_amqp(amqp: BaseAmqp, mocker: MockFixture):
        mocker.patch.object(amqp, '_channel', CoroutineMock())

        return amqp

    @pytest.mark.asyncio
    async def test_ok_declare_exchange(self, patched_amqp: BaseAmqp):
        await patched_amqp.declare_exchange()

        patched_amqp._channel.exchange_declare.assert_called_once_with(
            exchange_name=patched_amqp._exchange,
            type_name=patched_amqp._exchange_type,
            durable=True
        )

    @pytest.mark.asyncio
    async def test_ok_declare_queue(self, patched_amqp: BaseAmqp):
        await patched_amqp.declare_queue()

        patched_amqp._channel.queue_declare.assert_called_once_with(
            queue_name=patched_amqp._queue,
            durable=True
        )

    @pytest.mark.asyncio
    async def test_ok_bind_queue(self, patched_amqp: BaseAmqp):
        await patched_amqp.bind_queue()

        patched_amqp._channel.queue_bind.assert_called_once_with(
            exchange_name=patched_amqp._exchange,
            queue_name=patched_amqp._queue,
            routing_key=patched_amqp._routing_key
        )

    @pytest.mark.asyncio
    async def test_ok_specify_basic_qos(self, patched_amqp: BaseAmqp):
        await patched_amqp.specify_basic_qos()

        patched_amqp._channel.basic_qos.assert_called_once_with(
            prefetch_count=patched_amqp._prefetch_count,
            prefetch_size=patched_amqp._prefetch_size,
            connection_global=False
        )


class TestBaseAmqpClose:
    @pytest.mark.asyncio
    async def test_ok(self, amqp: BaseAmqp, mocker: MockFixture):
        mocker.patch.object(amqp, '_transport')
        mocker.patch.object(amqp, '_protocol', CoroutineMock(
            state=mocker.Mock(__eq__=mocker.Mock(return_value=True))
        ))

        await amqp.close()

        amqp._protocol.close.assert_called_once_with()
        amqp._transport.close.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_ok_wo_protocol(self, amqp: BaseAmqp, mocker: MockFixture):
        mocker.patch.object(amqp, '_transport')
        mocker.patch.object(amqp, '_protocol', CoroutineMock(
            is_open=False
        ))

        await amqp.close()

        amqp._protocol.close.assert_not_called()
        amqp._transport.close.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_ok_wo_transport(self, amqp: BaseAmqp, mocker: MockFixture):
        mocker.patch.object(amqp, '_transport', None)
        mocker.patch.object(amqp, '_protocol', CoroutineMock(
            state=mocker.Mock(__eq__=mocker.Mock(return_value=True))
        ))
        await amqp.close()

        amqp._protocol.close.assert_called_once_with()
        with pytest.raises(AttributeError):
            amqp._transport.close()


class TestBaseAmqpIsConnected:
    def test_ok_is_connected(self, amqp: BaseAmqp, mocker: MockFixture):
        mocker.patch.object(amqp, '_protocol', CoroutineMock(
            state=mocker.Mock(__eq__=mocker.Mock(return_value=True))
        ))
        mocker.patch.object(amqp, '_transport')

        assert amqp.is_connected

    def test_ok_is_not_connected_no_protocol(self, amqp: BaseAmqp, mocker: MockFixture):
        mocker.patch.object(amqp, '_protocol', None)
        mocker.patch.object(amqp, '_transport')

        assert not amqp.is_connected

    def test_ok_is_not_connected_no_transport(self, amqp: BaseAmqp, mocker: MockFixture):
        mocker.patch.object(amqp, '_protocol')
        mocker.patch.object(amqp, '_transport', None)

        assert not amqp.is_connected

    def test_ok_is_not_connected_protocol_is_not_open(self, amqp: BaseAmqp, mocker: MockFixture):
        mocker.patch.object(amqp, '_protocol', is_open=None)
        mocker.patch.object(amqp, '_transport')

        assert not amqp.is_connected


class TestBaseAmqpSerializer:
    @staticmethod
    @pytest.fixture
    def patched_amqp(amqp, mocker: MockFixture):
        mocker.patch.object(amqp, 'serializer')

        return amqp

    def test_ok_deserialize_data(self, patched_amqp: BaseAmqp, mocker: MockFixture):
        fake_data = mocker.Mock()
        patched_amqp.deserialize_data(fake_data)

        patched_amqp.serializer.deserialize.assert_called_once_with(fake_data)

    def test_ok_serialize_data(self, patched_amqp: BaseAmqp, mocker: MockFixture):
        fake_data = mocker.Mock()
        patched_amqp.serialize_data(fake_data)

        patched_amqp.serializer.serialize.assert_called_once_with(fake_data)
