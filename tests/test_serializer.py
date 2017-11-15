# -*- coding: utf-8 -*-

from datetime import datetime
import json

import msgpack
import pytest
from pytest_mock import MockFixture

from aioamqp_ext.serializer import (
    BaseSerializer,
    get_serializer,
    JSON,
    JsonSerializer,
    MSGPACK,
    MsgPackSerializer,
    SerializeException,
    DeserializeException,
)


class TestGetSerializer:
    def test_ok_json(self):
        serializer = get_serializer(JSON)

        assert issubclass(serializer, JsonSerializer)

    def test_ok_msgpack(self):
        serializer = get_serializer(MSGPACK)

        assert issubclass(serializer, MsgPackSerializer)

    def test_error_unknown(self):
        with pytest.raises(LookupError):
            get_serializer(None)


class TestJsonSerializer:
    @staticmethod
    @pytest.fixture
    def serializer():
        return get_serializer(JSON)

    def test_ok_serialize(self, serializer: JsonSerializer, mocker: MockFixture):
        fake_data = []

        mocked_json_dumps = mocker.patch('json.dumps')
        expected_value = mocked_json_dumps.return_value
        compared_value = serializer.serialize(fake_data)

        assert compared_value == expected_value

        mocked_json_dumps.assert_called_once_with(
            fake_data,
            default=JsonSerializer.datetime_converter
        )

    @pytest.mark.parametrize('err', [
        (TypeError,),
        (ValueError,),
    ])
    def test_fail_serialize(self, serializer: JsonSerializer, mocker: MockFixture, err):
        fake_data = []

        mocked_json_dumps = mocker.patch('json.dumps', side_effect=err)
        with pytest.raises(SerializeException):
            serializer.serialize(fake_data)

        mocked_json_dumps.assert_called_once_with(
            fake_data,
            default=JsonSerializer.datetime_converter
        )

    def test_ok_deserialize(self, serializer: JsonSerializer, mocker: MockFixture):
        fake_data = '{}'

        mocked_json_loads = mocker.patch('json.loads')
        expected_value = mocked_json_loads.return_value
        compared_value = serializer.deserialize(fake_data)

        assert compared_value == expected_value

        mocked_json_loads.assert_called_once_with(fake_data)

    def test_fail_deserialize(self, serializer: JsonSerializer, mocker: MockFixture):
        fake_data = '{}'
        mocked_json_loads = mocker.patch('json.loads', side_effect=json.JSONDecodeError('test', 'test', 0))

        with pytest.raises(DeserializeException):
            serializer.deserialize(fake_data)

        mocked_json_loads.assert_called_once_with(fake_data)


class TestMsgPackSerializer:
    @staticmethod
    @pytest.fixture
    def serializer():
        return get_serializer(MSGPACK)

    def test_ok_serialize(self, serializer: MsgPackSerializer, mocker: MockFixture):
        fake_data = []

        mocked_msgpack_dumps = mocker.patch('msgpack.dumps')
        expected_value = mocked_msgpack_dumps.return_value
        compared_value = serializer.serialize(fake_data)

        assert compared_value == expected_value

        mocked_msgpack_dumps.assert_called_once_with(
            fake_data,
            encoding='utf-8',
            default=MsgPackSerializer.datetime_converter
        )

    def test_fail_serialize(self, serializer: MsgPackSerializer, mocker: MockFixture):
        fake_data = []
        mocked_msgpack_dumps = mocker.patch('msgpack.dumps', side_effect=msgpack.PackException)

        with pytest.raises(SerializeException):
            serializer.serialize(fake_data)

        mocked_msgpack_dumps.assert_called_once_with(
            fake_data,
            encoding='utf-8',
            default=MsgPackSerializer.datetime_converter
        )

    def test_ok_deserialize(self, serializer: MsgPackSerializer, mocker: MockFixture):
        fake_data = '{}'

        mocked_json_loads = mocker.patch('msgpack.loads')
        expected_value = mocked_json_loads.return_value
        compared_value = serializer.deserialize(fake_data)

        assert compared_value == expected_value

        mocked_json_loads.assert_called_once_with(fake_data, encoding='utf-8')

    def test_fail_deserialize(self, serializer: MsgPackSerializer, mocker: MockFixture):
        fake_data = '{}'
        mocked_json_loads = mocker.patch('msgpack.loads', side_effect=msgpack.UnpackException)

        with pytest.raises(DeserializeException):
            serializer.deserialize(fake_data)

        mocked_json_loads.assert_called_once_with(fake_data, encoding='utf-8')


class TestBaseSerializer:
    def test_ok_datetime_converter(self, mocker: MockFixture):
        fake_datetime = mocker.Mock(spec=datetime)
        mocked_mktime = mocker.patch('time.mktime')

        expected_value = mocked_mktime.return_value
        compared_value = BaseSerializer.datetime_converter(fake_datetime)

        assert compared_value == expected_value

        fake_datetime.timetuple.assert_called_once_with()
