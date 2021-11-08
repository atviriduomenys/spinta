from typing import List, Dict, Any

import datetime

from authlib.oauth2 import rfc6749
from starlette.requests import Request

from spinta import commands
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

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return None

    def create_message(
        self,
        *,
        ns: str = None,
        model: str = None,
        prop: str = None,
        txn: str = None,
        action: str = None,
        query: Dict[str, Any] = None,
        method: str = None,
        reason: str = None,
        id_: str = None,
        rev: str = None,
    ):
        message = {
            'agent': self.agent,
            'client': self.client,
            'rctype': self.content_type,
            'format': self.format,
            'action': action,
            'method': method or self.method,
            'url': self.url,
            'reason': reason or self.reason,
            'time': datetime.datetime.now(datetime.timezone.utc).isoformat(),
            'txn': txn,
            'ns': ns,
            'model': model,
            'prop': prop,
            'query': query,
            'id': id_,
            'rev': rev,
        }
        return {k: v for k, v in message.items() if v is not None}

    def log(
        self,
        *,
        ns: str = None,
        model: str = None,
        prop: str = None,
        txn: str = None,
        action: str = None,
        query: Dict[str, Any] = None,
        method: str = None,
        reason: str = None,
        id_: str = None,
        rev: str = None,
    ):
        raise NotImplementedError


@commands.load.register(Context, AccessLog, Config)
def load(context: Context, accesslog: AccessLog, config: Config):
    accesslog.buffer_size = config.rc.get('accesslog', 'buffer_size', required=True)


@commands.load.register(Context, AccessLog, rfc6749.TokenMixin)
def load(context: Context, accesslog: AccessLog, token: rfc6749.TokenMixin):  # noqa
    accesslog.client = token.get_sub()


@commands.load.register(Context, AccessLog, Request)
def load(context: Context, accesslog: AccessLog, request: Request):  # noqa
    accesslog.method = request.method
    accesslog.url = str(request.url)
    accesslog.content_type = request.headers.get('content-type')
    accesslog.agent = request.headers.get('user-agent')


@commands.load.register(Context, AccessLog, UrlParams)
def load(context: Context, accesslog: AccessLog, params: UrlParams):  # noqa
    accesslog.format = params.format


def create_accesslog(
    context: Context,
    *,
    method: str = None,
    loaders: List[Any],
) -> AccessLog:
    config = context.get('config')
    accesslog = config.AccessLog()
    accesslog.method = method
    for loader in loaders:
        commands.load(context, accesslog, loader)
    return accesslog
