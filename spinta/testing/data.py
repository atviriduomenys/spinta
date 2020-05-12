from typing import List, Tuple, Union

import operator

import requests

from spinta.utils.schema import NA
from spinta.testing.client import TestClient


def listdata(
    resp: requests.Response,
    *keys: Tuple[str],
    sort: Union[bool, str] = True,
) -> List[tuple]:
    if resp.headers['content-type'].startswith('text/html'):
        data = resp.context
        assert resp.status_code == 200, data
        assert 'data' in data, data
        assert 'header' in data, data
        keys = keys or [k for k in data['header'] if not k.startswith('_')]
        data = [
            {k: v['value'] for k, v in zip(data['header'], row)}
            for row in data['data']
        ]
    else:
        data = resp.json()
        assert resp.status_code == 200, data
        assert '_data' in data, data
        data = data['_data']
        keys = keys or sorted({k for d in data for k in d if not k.startswith('_')})
    if len(keys) == 1:
        k = keys[0]
        data = [row.get(k, NA) for row in data]
    else:
        data = [tuple(row.get(k, NA) for k in keys) for row in data]
    if sort is True:
        data = sorted(data)
    elif sort:
        data = sorted(data, key=operator.itemgetter(keys.index(sort)))
    return data


def pushdata(app: TestClient, data: Union[List[dict], dict]):
    if isinstance(data, list):
        data = {'_data': data}
    resp = app.post('/', json=data)
    data = resp.json()
    assert resp.status_code == 200, data
    data = sorted(data['_data'], key=lambda x: x['_id'])
    return data
