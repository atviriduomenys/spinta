import json

import pytest


@pytest.mark.models(
    'backends/postgres/report/:dataset/test',
)
def test_push_same_model(model, app):
    app.authmodel(model, ['insert'])
    data = [
        {'_op': 'insert', '_type': model, 'status': 'ok'},
        {'_op': 'insert', '_type': model, 'status': 'warning'},
        {'_op': 'insert', '_type': model, 'status': 'critical'},
        {'_op': 'insert', '_type': model, 'status': 'blocker'},
    ]
    headers = {'content-type': 'application/x-ndjson'}
    payload = (json.dumps(x) + '\n' for x in data)
    resp = app.post('/', headers=headers, data=payload)
    resp = resp.json()
    data = resp.pop('_data')
    assert resp == {
        '_transaction': resp['_transaction'],
        '_status': 'ok',
    }
    assert len(data) == 4
    assert data[0] == {
        '_id': data[0]['_id'],
        '_revision': data[0]['_revision'],
        '_type': 'backends/postgres/report/:dataset/test',
        'count': None,
        'notes': [],
        'operating_licenses': [],
        'report_type': None,
        'revision': None,
        'status': 'ok',
        'update_time': None,
        'valid_from_date': None,
    }


def test_push_different_models(app):
    app.authmodel('country/:dataset/csv/:resource/countries', ['insert'])
    app.authmodel('backends/postgres/report/:dataset/test', ['insert'])
    data = [
        {'_op': 'insert', '_type': 'country/:dataset/csv', 'id': 'lt', 'code': 'lt'},
        {'_op': 'insert', '_type': 'backends/postgres/report/:dataset/test', 'status': 'ok'},
    ]
    headers = {'content-type': 'application/x-ndjson'}
    payload = (json.dumps(x) + '\n' for x in data)
    resp = app.post(f'/', headers=headers, data=payload)
    resp = resp.json()
    assert '_data' in resp, resp
    data = resp.pop('_data')
    assert resp == {
        '_transaction': resp.get('_transaction'),
        '_status': 'ok',
    }
    assert len(data) == 2

    d = data[0]
    assert d == {
        '_id': d['_id'],
        '_revision': d['_revision'],
        '_type': 'country/:dataset/csv/:resource/countries',
        'code': 'lt',
        'title': None,
    }

    d = data[1]
    assert d == {
        '_id': d['_id'],
        '_revision': d['_revision'],
        '_type': 'backends/postgres/report/:dataset/test',
        'count': None,
        'notes': [],
        'operating_licenses': [],
        'report_type': None,
        'revision': None,
        'status': 'ok',
        'update_time': None,
        'valid_from_date': None,
    }
