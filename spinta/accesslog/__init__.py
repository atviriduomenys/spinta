import datetime
import time
from typing import Any
from typing import Dict
from typing import Iterable
from typing import Iterator
from typing import List
from typing import Optional
from typing import TypeVar
from typing import AsyncIterator
from typing import overload

import psutil
from starlette.requests import Request

from spinta import commands
from spinta.auth import Token
from spinta.components import Context, Config
from spinta.components import UrlParams


class AccessLog:
    client: str = None
    method: str = None
    reason: str = None
    url: str = None
    buffer_size: int = 100
    format: str = None          # response format
    content_type: str = None    # request content-type header
    agent: str = None           # request user-agent header
    txn: str = None             # request transaction id
    start: float = None         # request start time in secodns
    memory: int = None          # memory used in bytes at the start of request

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return None

    def log(self, message: Dict[str, Any]) -> None:
        raise NotImplementedError

    def request(
        self,
        *,
        txn: str,
        ns: str = None,
        model: str = None,
        prop: str = None,
        action: str = None,
        method: str = None,
        reason: str = None,
        id_: str = None,
        rev: str = None,
    ):
        self.txn = txn
        self.start = self._get_time()
        self.memory = self._get_memory()
        message = {
            'time': datetime.datetime.now(datetime.timezone.utc).isoformat(),
            'type': 'request',
            'method': method or self.method,
            'action': action,
            'ns': ns,
            'model': model,
            'prop': prop,
            'id': id_,
            'rev': rev,
            'txn': txn,
            'rctype': self.content_type,
            'format': self.format,
            'url': self.url,
            'client': self.client,
            'reason': reason or self.reason,
            'agent': self.agent,
        }
        message = {k: v for k, v in message.items() if v is not None}
        self.log(message)

    def response(
        self,
        *,
        objects: int,
    ):
        delta = self._get_time() - self.start
        memory = self._get_memory() - self.memory
        message = {
            'time': datetime.datetime.now(datetime.timezone.utc).isoformat(),
            'type': 'response',
            'delta': delta,
            'memory': memory,
            'objects': objects,
            'txn': self.txn,
        }

        self.log(message)

    def _get_time(self) -> float:
        "Get current time in seconds"
        return time.monotonic()

    def _get_memory(self) -> float:
        "Get currently used memory in bytes"
        return psutil.Process().memory_info().rss


@overload
@commands.load.register(Context, AccessLog, Config)
def load(context: Context, accesslog: AccessLog, config: Config):
    accesslog.buffer_size = config.rc.get(
        'accesslog', 'buffer_size', required=True,
    )


@overload
@commands.load.register(Context, AccessLog, Token)
def load(context: Context, accesslog: AccessLog, token: Token):  # noqa
    accesslog.client = token.get_sub()


@overload
@commands.load.register(Context, AccessLog, Request)
def load(context: Context, accesslog: AccessLog, request: Request):  # noqa
    accesslog.method = request.method
    accesslog.url = str(request.url)
    accesslog.content_type = request.headers.get('content-type')
    accesslog.agent = request.headers.get('user-agent')


@overload
@commands.load.register(Context, AccessLog, UrlParams)
def load(context: Context, accesslog: AccessLog, params: UrlParams):  # noqa
    accesslog.format = params.format


def create_accesslog(
    context: Context,
    *,
    method: str = None,
    loaders: List[Any],
) -> AccessLog:
    config: Config = context.get('config')
    # XXX: Probably we should clone store.accesslog here, instead of creating
    #      completely new AccessLog instance.
    accesslog: AccessLog = config.AccessLog()
    accesslog.method = method
    for loader in loaders:
        commands.load(context, accesslog, loader)
    return accesslog


TRow = TypeVar('TRow')


def log_response(
    context: Context,
    rows: Iterable[TRow],
) -> Iterator[TRow]:
    objects = 0
    for row in rows:
        objects += 1
        yield row
    accesslog: AccessLog = context.get('accesslog')
    accesslog.response(objects=objects)


async def log_async_response(
    context: Context,
    rows: AsyncIterator[TRow],
) -> AsyncIterator[TRow]:
    objects = 0
    async for row in rows:
        objects += 1
        yield row
    accesslog: AccessLog = context.get('accesslog')
    accesslog.response(objects=objects)
