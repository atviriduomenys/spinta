from __future__ import annotations

import base64
import json
import operator
from textwrap import indent
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Union
from typing import cast
from typing import NamedTuple
from typing import Optional

import httpx
import requests
from pprintpp import pformat

from spinta.formats.html.components import Cell
from spinta.testing.client import TestClient
from spinta.utils.data import take
from spinta.utils.nestedstruct import flatten


def get_keys_for_row(keys, row: dict):
    return keys or sorted({
        k
        for d in flatten(row, omit_none=False)
        for k in d
        if not k.startswith('_')
    })


def listdata(
    resp: Union[httpx.Response, List[Dict[str, Any]]],
    *keys: Union[str, Callable[[], bool]],
    sort: Union[bool, str] = True,
    full: bool = False,  # returns dicts instead of tuples
) -> List[tuple]:
    """Return data from a given Response object.

    Only non reserved fields are returned.

    This function returns:

        List[Tuple[Any]]
            This is returned by default.

        List[Any]
            This is returned if one `keys` key is given.

        List[Dict[str, Any]]
            This is returned if `full` is True.

        List[Dict[str, Any]] (flattened)
            This is returned if `full` is True and `keys` are given.

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
    old_keys = keys
    # Prepare data
    if isinstance(resp, list):
        data = resp

    elif resp.headers['content-type'].startswith('text/html'):
        data = resp.context
        assert resp.status_code >= 200 and resp.status_code < 400, pformat({
            'status': resp.status_code,
            'resp': data,
        })
        assert 'data' in data, pformat(data)
        assert 'header' in data, pformat(data)
        header = data['header']
        keys = keys or [k for k in header if not k.startswith('_')]
        data = [
            {k: v.value for k, v in zip(header, row)}
            for row in cast(List[List[Cell]], data['data'])
        ]

    else:
        data = resp.json()
        assert resp.status_code >= 200 and resp.status_code < 400, pformat({
            'status': resp.status_code,
            'resp': data,
        })
        if '_id' in data:
            data = [data]
        else:
            assert '_data' in data, pformat(data)
            data = data['_data']

        keys = get_keys_for_row(old_keys, data)


    # Clean data
    if full:
        data = [take(get_keys_for_row(old_keys, row), row) for row in data]
    elif len(keys) == 1:
        k = keys[0]
        data = [take(k, row) for row in data]
    else:
        data = [tuple(take(k, row) for k in keys) for row in data]

    # Sort
    if sort is True:
        data = sorted(data, key=str)
    elif sort:
        if full:
            sort_key = operator.itemgetter(sort)
        else:
            sort_key = operator.itemgetter(keys.index(sort))
        data = sorted(data, key=sort_key)

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


class Data(NamedTuple):
    id: str
    rev: str
    data: Dict[str, Any]

    @property
    def sid(self):
        if self.id:
            return self.id[:8]

    def update(self, data: Dict[str, Any]) -> Data:
        return Data(id=self.id, rev=self.rev, data={**self.data, **data})


def _obj_from_dict(data: Dict[str, Any]) -> Data:
    return Data(
        id=data.pop('_id', None),
        rev=data.pop('_revision', None),
        data=data,
    )


def send(
    app: TestClient,
    model: str,
    action: str,
    obj: Optional[Union[Data, Dict[str, Any]]] = None,
    data: Optional[Dict[str, Any]] = None,
    *,
    select: Optional[List[str]] = None,
    sort: Optional[Union[bool, str]] = None,
    full: Optional[bool] = None,  # returns dicts instead of tuples
) -> Union[Data, List[Data]]:
    if obj is not None and isinstance(obj, dict):
        obj = _obj_from_dict(obj)

    if data is not None:
        obj = obj.update(data)

    if action == 'insert':
        resp = app.post(model, json=obj.data)
    elif action in 'patch':
        resp = app.patch(f'{model}/{obj.id}', json={
            **obj.data,
            '_revision': obj.rev,
        })
    elif action in 'delete':
        resp = app.delete(f'{model}/{obj.id}')
    elif action == 'changes':
        if obj:
            resp = app.get(f'{model}/{obj.id}/:changes')
        else:
            resp = app.get(f'{model}/:changes')
    elif action == 'getall':
        resp = app.get(model)
    elif action == 'getone':
        resp = app.get(f'{model}/{obj.id}')
    else:
        resp = app.get(f'{model}/{action}')

    if select is not None or sort is not None or full is not None:
        if select is None:
            select = []
        if sort is None:
            sort = False
        if full is None:
            full = True
        return listdata(resp, *select, sort=sort, full=full)

    try:
        resp.raise_for_status()
    except httpx.HTTPStatusError as e:
        dump = indent(pformat(obj.data), '  ').strip()
        raise Exception(
            f"send error:\n"
            f"  model={model},\n"
            f"  action={action},\n"
            f"  error={e},\n  "
            f"data: {dump}"
        )

    result = resp.json()

    if '_data' in result:
        return [_obj_from_dict(data) for data in result['_data']]

    else:
        return _obj_from_dict(result)


def encode_page_values_manually(row: dict):
    return base64.urlsafe_b64encode(json.dumps(list(row.values())).encode('ascii')).decode('ascii')


def are_lists_of_dicts_equal(list1, list2):
    # Check if the lengths of the lists are the same
    if len(list1) != len(list2):
        return False

    # Convert each list of dictionaries to a set
    set1 = {frozenset(d.items()) for d in list1}
    set2 = {frozenset(d.items()) for d in list2}

    # Compare the sets
    return set1 == set2
