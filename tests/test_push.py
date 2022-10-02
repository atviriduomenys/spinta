import json
import hashlib

import pytest

from spinta.cli.push import _PushRow
from spinta.cli.push import _get_row_for_error
from spinta.cli.push import _map_sent_and_recv 
from spinta.core.config import RawConfig
from spinta.testing.manifest import load_manifest


@pytest.mark.skip('datasets')
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
    resp = app.post('/', headers=headers, content=payload)
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


def sha1(s):
    return hashlib.sha1(s.encode()).hexdigest()


@pytest.mark.skip('datasets')
def test_push_different_models(app):
    app.authorize(['spinta_set_meta_fields'])
    app.authmodel('country/:dataset/csv/:resource/countries', ['insert'])
    app.authmodel('backends/postgres/report/:dataset/test', ['insert'])
    data = [
        {'_op': 'insert', '_type': 'country/:dataset/csv', '_id': sha1('lt'), 'code': 'lt'},
        {'_op': 'insert', '_type': 'backends/postgres/report/:dataset/test', 'status': 'ok'},
    ]
    headers = {'content-type': 'application/x-ndjson'}
    payload = (json.dumps(x) + '\n' for x in data)
    resp = app.post('/', headers=headers, data=payload)
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


def test__map_sent_and_recv__no_recv(rc: RawConfig):
    manifest = load_manifest(rc, '''
    d | r | b | m | property | type   | access
    datasets/gov/example     |        |
      |   |   | Country      |        |
      |   |   |   | name     | string | open
    ''')

    model = manifest.models['datasets/gov/example/Country']
    sent = [
        _PushRow(model, {'name': 'Vilnius'}),
    ]
    recv = None
    assert list(_map_sent_and_recv(sent, recv)) == sent


def test__get_row_for_error__errors(rc: RawConfig):
    manifest = load_manifest(rc, '''
    d | r | b | m | property | type   | access
    datasets/gov/example     |        |
      |   |   | Country      |        |
      |   |   |   | name     | string | open
    ''')

    model = manifest.models['datasets/gov/example/Country']
    rows = [
        _PushRow(model, {
            '_id': '4d741843-4e94-4890-81d9-5af7c5b5989a',
            'name': 'Vilnius',
        }),
    ]
    errors = [
        {
            'context': {
                'id': '4d741843-4e94-4890-81d9-5af7c5b5989a',
            }
        }
    ]
    assert _get_row_for_error(rows, errors).splitlines() == [
        ' Model datasets/gov/example/Country, data:',
        " {'_id': '4d741843-4e94-4890-81d9-5af7c5b5989a', 'name': 'Vilnius'}",
    ]
