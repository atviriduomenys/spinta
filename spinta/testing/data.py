from typing import List, Tuple, Union

import operator

import requests

from spinta.utils.data import take
from spinta.testing.client import TestClient
from spinta.utils.nestedstruct import flatten


def listdata(
    resp: requests.Response,
    *keys: str,
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
        keys = keys or sorted({
            k
            for d in flatten(data)
            for k in d
            if not k.startswith('_')
        })
    if len(keys) == 1:
        k = keys[0]
        data = [take(k, row) for row in data]
    else:
        data = [tuple(take(k, row) for k in keys) for row in data]
    if sort is True:
        data = sorted(data)
    elif sort:
        data = sorted(data, key=operator.itemgetter(keys.index(sort)))
    return data


def pushdata(app: TestClient, *args) -> Union[dict, list]:
    model: str
    data: Union[list, dict]
    if len(args) == 2:
        model, data = args
    else:
        model = '/'
        data, = args
    multiple = isinstance(data, list)
    if multiple:
        data = {'_data': data}
        resp = app.post(model, json=data)
    else:
        resp = app.post(model, json=data)
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        raise Exception(
            f"pushdata error: model={model}, error={e}, data: {data!r}"
        )
    data = resp.json()
    if multiple:
        return sorted(data['_data'], key=lambda x: x['_id'])
    else:
        return data
