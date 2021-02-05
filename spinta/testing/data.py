import operator
from textwrap import indent
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Union

import requests
from pprintpp import pformat

from spinta.testing.client import TestClient
from spinta.utils.data import take
from spinta.utils.nestedstruct import flatten


def listdata(
    resp: Union[requests.Response, List[Dict[str, Any]]],
    *keys: Union[str, Callable[[], bool]],
    sort: Union[bool, str] = True,
    full: bool = False,
) -> List[tuple]:
    """Return data from a given requests.Response object.

    Only non reserved fields are returned.

    By default data are converted to List[Tuple[Any]], but if `full` is True,
    then List[Dict[str, Any] is returned.

    Usage:

        >>> data = [
        ...     {'_id': 1, 'odd': 1, 'even': 2},
        ...     {'_id': 2, 'odd': 3, 'even': 4},
        ...     {'_id': 3, 'odd': 5, 'even': 6},
        ... }

        >>> listdata(data)
        [
            (1, 2),
            (3, 4),
            (5, 6),
        ]

        >>> listdata(data, 'even')
        [2, 4, 6]

        >>> listdata(data, 'odd', 'even')
        [
            (1, 2),
            (3, 4),
            (5, 6),
        ]

        >>> listdata(data, full=True)
        data = [
            {'odd': 1, 'even': 2},
            {'odd': 3, 'even': 4},
            {'odd': 5, 'even': 6},
        }

    """

    # Prepare data
    if isinstance(resp, list):
        data = resp

    elif resp.headers['content-type'].startswith('text/html'):
        data = resp.context
        assert resp.status_code == 200, pformat(data)
        assert 'data' in data, pformat(data)
        assert 'header' in data, pformat(data)
        keys = keys or [k for k in data['header'] if not k.startswith('_')]
        data = [
            {k: v['value'] for k, v in zip(data['header'], row)}
            for row in data['data']
        ]

    else:
        data = resp.json()
        assert resp.status_code == 200, pformat(data)
        assert '_data' in data, pformat(data)
        data = data['_data']
        keys = keys or sorted({
            k
            for d in flatten(data)
            for k in d
            if not k.startswith('_')
        })

    # Clean data
    if full:
        data = [take(keys, row) for row in data]
    elif len(keys) == 1:
        k = keys[0]
        data = [take(k, row) for row in data]
    else:
        data = [tuple(take(k, row) for k in keys) for row in data]

    # Sort
    if sort is True:
        data = sorted(data, key=str)
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
        dump = indent(pformat(data), '  ').strip()
        raise Exception(
            f"pushdata error:\n"
            f"  model={model},\n"
            f"  error={e},\n  "
            f"data: {dump}"
        )
    data = resp.json()
    if multiple:
        return sorted(data['_data'], key=lambda x: x['_id'])
    else:
        return data
