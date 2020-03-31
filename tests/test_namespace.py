import json
import operator
import hashlib

import pytest


@pytest.mark.skip('todo')
@pytest.mark.models(
    'backends/postgres/%s/:dataset/test',
)
def test_getall(model, app):
    continent = model % 'continent'
    country = model % 'country'
    capital = model % 'capital'

    app.authorize([
        'spinta_set_meta_fields',
        'spinta_getall',
    ])
    app.authmodel(continent, ['insert'])
    app.authmodel(country, ['insert'])
    app.authmodel(capital, ['insert'])

    data = [
        {
            '_op': 'insert',
            '_type': continent,
            '_id': '356a192b7913b04c54574d18c28d46e6395428ab',
            'title': 'Europe',
        },
        {
            '_op': 'insert',
            '_type': country,
            '_id': 'da4b9237bacccdf19c0760cab7aec4a8359010b0',
            'continent': '356a192b7913b04c54574d18c28d46e6395428ab',
            'code': 'lt',
            'title': 'Lithuania',
        },
        {
            '_op': 'insert',
            '_type': capital,
            '_id': '77de68daecd823babbb58edb1c8e14d7106e83bb',
            'country': 'da4b9237bacccdf19c0760cab7aec4a8359010b0',
            'title': 'Vilnius',
        },
    ]
    headers = {'content-type': 'application/x-ndjson'}
    payload = (json.dumps(d) + '\n' for d in data)
    resp = app.post('/', headers=headers, data=payload)
    assert resp.status_code == 200
    data = {d['_id']: d for d in resp.json()['_data']}

    resp = app.get('/:all/:dataset/test')
    assert resp.status_code == 200
    assert sorted(resp.json()['_data'], key=operator.itemgetter('_id')) == [
        {
            '_id': '356a192b7913b04c54574d18c28d46e6395428ab',
            '_revision': data['356a192b7913b04c54574d18c28d46e6395428ab']['_revision'],
            '_type': 'backends/postgres/continent/:dataset/test',
            'title': 'Europe',
        },
        {
            '_id': '77de68daecd823babbb58edb1c8e14d7106e83bb',
            '_revision': data['77de68daecd823babbb58edb1c8e14d7106e83bb']['_revision'],
            '_type': 'backends/postgres/capital/:dataset/test',
            'country': 'da4b9237bacccdf19c0760cab7aec4a8359010b0',
            'title': 'Vilnius',
        },
        {
            '_id': 'da4b9237bacccdf19c0760cab7aec4a8359010b0',
            '_revision': data['da4b9237bacccdf19c0760cab7aec4a8359010b0']['_revision'],
            '_type': 'backends/postgres/country/:dataset/test',
            'code': 'lt',
            'continent': '356a192b7913b04c54574d18c28d46e6395428ab',
            'title': 'Lithuania',
        },
    ]

    resp = app.get('/:all/:dataset/csv')
    assert resp.status_code == 200
    assert resp.json()['_data'] == []


def sha1(s):
    return hashlib.sha1(s.encode()).hexdigest()


@pytest.mark.skip('todo')
@pytest.mark.models(
    'backends/postgres/%s/:dataset/test',
)
def test_getall_ns(model, app):
    continent = model % 'continent'
    country = model % 'country'
    capital = model % 'capital'

    app.authorize([
        'spinta_set_meta_fields',
        'spinta_getall',
    ])
    app.authmodel(continent, ['insert'])
    app.authmodel(country, ['insert'])
    app.authmodel(capital, ['insert'])

    data = [
        {
            '_op': 'insert',
            '_type': continent,
            '_id': sha1('1'),
            'title': 'Europe',
        },
        {
            '_op': 'insert',
            '_type': country,
            '_id': sha1('2'),
            'continent': sha1('1'),
            'code': 'lt',
            'title': 'Lithuania',
        },
        {
            '_op': 'insert',
            '_type': capital,
            '_id': sha1('3'),
            'country': sha1('2'),
            'title': 'Vilnius',
        },
    ]
    headers = {'content-type': 'application/x-ndjson'}
    payload = (json.dumps(d) + '\n' for d in data)
    resp = app.post('/', headers=headers, data=payload)
    assert resp.status_code == 200
    data = {d['_id']: d for d in resp.json()['_data']}

    resp = app.get('/:ns/:all/:dataset/test')
    assert resp.status_code == 200
    assert [d['_id'] for d in resp.json()['_data']] == [
        'backends/postgres/capital/:dataset/test',
        'backends/postgres/capital/:dataset/test',
        'backends/postgres/continent/:dataset/test',
        'backends/postgres/continent/:dataset/test',
        'backends/postgres/country/:dataset/test',
        'backends/postgres/country/:dataset/test',
        'backends/postgres/org/:dataset/test',
        'backends/postgres/org/:dataset/test',
        'backends/postgres/report/:dataset/test',
        'backends/postgres/report/:dataset/test',
    ]
