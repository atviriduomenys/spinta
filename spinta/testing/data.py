from typing import List, Tuple, Union

import operator

import requests


def listdata(
    resp: requests.Response,
    *keys: Tuple[str],
    sort: Union[bool, str] = True,
) -> List[tuple]:
    data = resp.json()
    assert resp.status_code == 200, data
    assert '_data' in data, data
    data = data['_data']
    keys = keys or sorted({k for d in data for k in d if not k.startswith('_')})
    if len(keys) == 1:
        k = keys[0]
        data = [row[k] for row in data]
    else:
        data = [tuple(row[k] for k in keys) for row in data]
    if sort is True:
        data = sorted(data)
    elif sort:
        data = sorted(data, key=operator.itemgetter(keys.index(sort)))
    return data
