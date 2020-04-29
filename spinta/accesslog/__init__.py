from typing import List, Dict, Any

import datetime

from authlib.oauth2 import rfc6749
from starlette.requests import Request

from spinta import commands
from spinta.components import Context, Config


class AccessLog:
    accessors: List[Dict[str, str]]
    method: str = None
    reason: str = None
    url: str = None
    buffer_size: int = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return None

    def create_message(
        self,
        txn: str,
        method: str,
        reason: str,
        resources: List[Dict[str, Any]],
        fields: List[str],
    ):
        message = {
            'accessors': self.accessors,
            'http_method': method or self.method,
            'url': self.url,
            'reason': reason or self.reason,
            'timestamp': datetime.datetime.now(datetime.timezone.utc).isoformat(),
            'transaction_id': txn,
            'resources': resources,
            'fields': fields,
        }
        return message

    def log(
        self,
        *,
        txn: str = None,
        method: str = None,
        reason: str = None,
        resources: List[Dict[str, Any]],
        fields: List[str],
    ):
        raise NotImplementedError


@commands.load.register(Context, AccessLog, Config)
def load(context: Context, accesslog: AccessLog, config: Config):
    accesslog.buffer_size = config.rc.get('accesslog', 'buffer_size', required=True)


@commands.load.register(Context, AccessLog, rfc6749.TokenMixin)
def load(context: Context, accesslog: AccessLog, token: rfc6749.TokenMixin):  # noqa
    accesslog.accessors.extend([
        {
            'type': 'person',
            'id': token.get_sub(),
        },
        {
            'type': 'client',
            'id': token.get_aud(),
        },
    ])


@commands.load.register(Context, AccessLog, Request)
def load(context: Context, accesslog: AccessLog, request: Request):  # noqa
    accesslog.method = request.method
    accesslog.url = str(request.url)


def create_accesslog(
    context: Context,
    *,
    method: str = None,
    loaders: List[Any],
) -> AccessLog:
    config = context.get('config')
    accesslog = config.AccessLog()
    accesslog.method = method
    accesslog.accessors = []
    for loader in loaders:
        commands.load(context, accesslog, loader)
    return accesslog
