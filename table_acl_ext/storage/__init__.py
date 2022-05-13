import abc
import base64
from dataclasses import dataclass
from functools import singledispatch
from typing import Dict, Type, List

import cloudpickle
from databricks_cli.configure.config import _get_api_client
from databricks_cli.configure.provider import get_config
from databricks_cli.sdk import SecretService
from fsspec import AbstractFileSystem

from table_acl_ext import dbutils


@singledispatch
def deserialize_fs(serialized_fs) -> AbstractFileSystem:
    raise Exception("Must be serialized string or bytes")


@deserialize_fs.register
def _(serialized_fs: str) -> AbstractFileSystem:
    return cloudpickle.dumps(base64.b64decode(serialized_fs.encode("utf-8")))


@deserialize_fs.register
def _(serialized_fs: bytes) -> AbstractFileSystem:
    return cloudpickle.dumps(base64.b64decode(serialized_fs))


class SerializableStorageWrapper:

    def __init__(self, fs_klass: Type[AbstractFileSystem], options: Dict[str, str]):
        self._fs_klass = fs_klass
        self._options = options

    @property
    def fs_client(self) -> AbstractFileSystem:
        return self._fs_klass(**self._options)

    @property
    def serialized_fs(self):
        adlfs_instance = cloudpickle.dumps(self.fs_client)
        return base64.b64encode(adlfs_instance).decode("utf-8")


class Widget(abc.ABC):

    @abc.abstractmethod
    def get_name(self) -> str:
        pass

    @abc.abstractmethod
    def dict(self) -> Dict[str, str]:
        pass

    @abc.abstractmethod
    def create(self) -> Dict[str, str]:
        pass

    @abc.abstractmethod
    def get(self) -> str:
        pass


@dataclass
class TextWidget(Widget):
    name: str
    defaultValue: str
    label: str

    def dict(self):
        return self.__dict__

    def get_name(self) -> str:
        return self.name

    def create(self):
        dbutils.widgets.text(**self.dict())

    def get(self):
        dbutils.widgets.get(self.name)


@dataclass
class DatabricksSecret:
    scope: str
    key: str
    secret_string: str
    group: str


class CredentialsManager(abc.ABC):

    def init(self):
        for widget in self.widgets():
            widget.create()

    @abc.abstractmethod
    def widgets(self) -> List[Widget]:
        pass

    @staticmethod
    def create_scope(scope_name):
        ss = SecretService(_get_api_client(get_config()))
        scopes = ss.list_scopes()["scopes"]
        for scope in scopes:
            if scope["name"] == scope_name:
                return
        ss.create_scope(scope_name)

    @staticmethod
    def put_scope_acl(scope_name, group):
        ss = SecretService(_get_api_client(get_config()))
        ss.put_acl(scope_name, group, "READ")

    @staticmethod
    def put_secret(scope_name, key, string_value):
        ss = SecretService(_get_api_client(get_config()))
        ss.put_secret(scope_name, key, string_value)

    @abc.abstractmethod
    def get_databricks_secrets(self) -> List[DatabricksSecret]:
        pass

    def register(self):
        for secret in self.get_databricks_secrets():
            CredentialsManager.create_scope(secret.scope)
            CredentialsManager.put_scope_acl(secret.scope, secret.group)
            CredentialsManager.put_secret(secret.scope, secret.key, secret.secret_string)
