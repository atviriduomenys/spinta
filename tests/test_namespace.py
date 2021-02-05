import json
import hashlib
from typing import Tuple

import pytest

from spinta.testing.client import TestClient
from spinta.testing.data import listdata
from spinta.testing.data import pushdata
from spinta.utils.data import take


def _create_data(app: TestClient, ns: str) -> Tuple[str, str]:
    continent = ns + '/continent'
    country = ns + '/country'
    capital = ns + '/capital'

    app.authmodel(continent, ['insert'])
    app.authmodel(country, ['insert'])
    app.authmodel(capital, ['insert'])

    eu = take('_id', pushdata(app, continent, {
        'title': 'Europe',
    }))
    lt = take('_id', pushdata(app, country, {
        'code': 'lt',
        'title': 'Lithuania',
        'continent': {'_id': eu},
    }))
    pushdata(app, capital, {
        'title': 'Vilnius',
        'country': {'_id': lt},
    })

    return eu, lt


@pytest.mark.models('datasets/backends/postgres/dataset')
def test_getall(model: str, app: TestClient):
    eu, lt = _create_data(app, model)
    app.authorize(['spinta_getall'])

    resp = app.get('/datasets/backends/postgres/dataset/:all')
    assert listdata(resp, full=True) == [
        {'code': 'lt', 'continent._id': eu, 'title': 'Lithuania'},
        {'country._id': lt, 'title': 'Vilnius'},
        {'title': 'Europe'},
    ]

    resp = app.get('/datasets/csv/:all')
    assert listdata(resp) == []


def sha1(s):
    return hashlib.sha1(s.encode()).hexdigest()


@pytest.mark.models('datasets/backends/postgres/dataset')
def test_getall_ns(model, app):
    _create_data(app, model)
    app.authorize(['spinta_getall'])

    resp = app.get('/datasets/backends/postgres/dataset/:ns/:all')
    assert listdata(resp, '_id') == [
        'datasets/backends/postgres/dataset/capital',
        'datasets/backends/postgres/dataset/continent',
        'datasets/backends/postgres/dataset/country',
        'datasets/backends/postgres/dataset/org',
        'datasets/backends/postgres/dataset/report',
    ]
