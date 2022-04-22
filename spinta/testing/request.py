from typing import Dict
from typing import Optional

from starlette.requests import Request


def make_get_request(
    path: str,
    query: Optional[str] = None,
    headers: Optional[Dict[str, str]] = None,
):
    path = '/' + path
    params = {'path': path}
    if query:
        path = path + '?' + query
    return Request({
        'type': 'http',
        'method': 'GET',
        'path': path,
        'path_params': params,
        'headers': [
            (k.lower().encode(), v.encode())
            for k, v in (headers or {}).items()
        ],
    })
