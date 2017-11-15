# -*- coding: utf-8 -*-

import json
import time
from abc import ABC, abstractmethod
from datetime import datetime

import msgpack

__all__ = (
    'JSON',
    'MSGPACK',
    'get_serializer'
)


JSON = 'json'
MSGPACK = 'msgpack'


class DeserializeException(Exception):
    pass


class SerializeException(Exception):
    pass


class BaseSerializer(ABC):
    @staticmethod
    @abstractmethod
    def serialize(data):
        pass

    @staticmethod
    @abstractmethod
    def deserialize(data):
        pass

    @staticmethod
    def datetime_converter(obj):
        if isinstance(obj, datetime):
            return time.mktime(obj.timetuple())
        return obj


class MsgPackSerializer(BaseSerializer):
    @staticmethod
    def deserialize(data):
        try:
            return msgpack.loads(data, encoding='utf-8')
        except msgpack.UnpackException as e:
            raise DeserializeException(str(e))

    @staticmethod
    def serialize(data):
        try:
            return msgpack.dumps(data, encoding='utf-8', default=MsgPackSerializer.datetime_converter)
        except msgpack.PackException as e:
            raise SerializeException(str(e))


class JsonSerializer(BaseSerializer):
    @staticmethod
    def deserialize(data):
        try:
            return json.loads(data)
        except json.JSONDecodeError as e:
            raise DeserializeException(str(e))

    @staticmethod
    def serialize(data):
        try:
            return json.dumps(data, default=JsonSerializer.datetime_converter)
        except (TypeError, ValueError) as e:
            raise SerializeException(str(e))


def get_serializer(serializer=JSON):
    try:
        return {
            JSON: JsonSerializer,
            MSGPACK: MsgPackSerializer
        }[serializer]
    except KeyError:
        raise LookupError('Unknown serializer: {}'.format(serializer))
