import json
import operator

import pytest


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
            '_id': '1',
            'title': 'Europe',
        },
        {
            '_op': 'insert',
            '_type': country,
            '_id': '2',
            'continent': '1',
            'code': 'lt',
            'title': 'Lithuania',
        },
        {
            '_op': 'insert',
            '_type': capital,
            '_id': '3',
            'country': 2,
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
            '_id': '1',
            '_revision': data['1']['_revision'],
            '_type': 'backends/postgres/continent/:dataset/test',
            'title': 'Europe',
        },
        {
            '_id': '2',
            '_revision': data['2']['_revision'],
            '_type': 'backends/postgres/country/:dataset/test',
            'code': 'lt',
            'continent': '1',
            'title': 'Lithuania',
        },
        {
            '_id': '3',
            '_revision': data['3']['_revision'],
            '_type': 'backends/postgres/capital/:dataset/test',
            'country': 2,
            'title': 'Vilnius',
        },
    ]

    resp = app.get('/:all/:dataset/csv')
    assert resp.status_code == 200
    assert resp.json()['_data'] == []
