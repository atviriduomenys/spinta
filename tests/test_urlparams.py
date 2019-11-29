from starlette.requests import Request

from spinta.components import Context, UrlParams, Version
from spinta.commands import prepare


def _parse(context: Context, url: str, accept='application/json') -> UrlParams:
    params = UrlParams()
    if '?' in url:
        path, query = url.split('?')
    else:
        path, query = url, ''
    request = Request({
        'type': 'http',
        'path': path,
        'path_params': {
            'path': path,
        },
        'query_string': query.encode(),
        'headers': [
            (b'Accept', accept.encode()),
        ]
    })
    return prepare(context, params, Version(), request)


def test_format(context):
    assert _parse(context, '?format(width(42))').formatparams == {'width': 42}
    assert _parse(context, '?format(csv,width(42))').format == 'csv'
    assert _parse(context, '?format(csv,width(42))').formatparams == {'width': 42}
