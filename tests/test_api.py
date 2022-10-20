import uuid
from typing import Any
from typing import Dict
from typing import List
from typing import Tuple
from typing import cast

import pytest

from spinta.core.config import RawConfig
from spinta.formats.html.components import Cell
from spinta.formats.html.helpers import short_id
from spinta.testing.client import TestClient
from spinta.testing.client import TestClientResponse
from spinta.testing.client import get_html_tree
from spinta.testing.utils import get_error_codes, get_error_context
from spinta.testing.manifest import prepare_manifest
from spinta.utils.nestedstruct import flatten
from spinta.testing.client import create_test_client
from spinta.backends.memory.components import Memory
from spinta.testing.data import send


def _cleaned_context(
    resp: TestClientResponse,
    *,
    data: bool = True,
) -> Dict[str, Any]:
    context = resp.context.copy()
    if 'data' in context:
        context['data'] = [
            [cell.as_dict() for cell in row]
            for row in cast(List[List[Cell]], resp.context['data'])
        ]
    if 'row' in context:
        context['row'] = [
            (name, cell.as_dict())
            for name, cell in cast(Tuple[str, Cell], context['row'])
        ]

    if data:
        header = context['header']
        context['data'] = [
            {
                k: v['value']
                for k, v in zip(header, d)
            }
            for d in cast(List[List[Dict[str, Any]]], context['data'])
        ]
    del context['params']
    del context['zip']
    if 'request' in context:
        del context['request']
    return context


def test_version(app):
    resp = app.get('/version')
    assert resp.status_code == 200
    assert sorted(next(flatten(resp.json())).keys()) == [
        'api.version',
        'build',
        'implementation.name',
        'implementation.version',
    ]


def test_app(app):
    app.authorize(['spinta_getall'])
    resp = app.get('/', headers={'accept': 'text/html'})
    assert resp.status_code == 200, resp.text

    resp.context.pop('request')
    data = _cleaned_context(resp)
    assert data == {
        'location': [
            ('🏠', '/'),
        ],
        'empty': False,
        'header': ['name', 'title', 'description'],
        'data': data['data'],
        'formats': [
            ('CSV', '/:format/csv'),
            ('JSON', '/:format/json'),
            ('JSONL', '/:format/jsonl'),
            ('ASCII', '/:format/ascii'),
        ],
    }
    assert next(d for d in data['data'] if d['title'] == 'Country') == {
        'name': '📄 country',
        'title': 'Country',
        'description': '',
    }

    html = get_html_tree(resp)
    rows = html.cssselect('table.table tr td:nth-child(1)')
    rows = [row.text_content().strip() for row in rows]
    assert '📄 country' in rows
    assert '📁 datasets/' in rows


def test_directory(app):
    app.authorize(['spinta_datasets_getall'])
    resp = app.get('/datasets/xlsx/rinkimai/:ns', headers={'accept': 'text/html'})
    assert resp.status_code == 200

    resp.context.pop('request')
    assert _cleaned_context(resp, data=False) == {
        'location': [
            ('🏠', '/'),
            ('datasets', '/datasets'),
            ('xlsx', '/datasets/xlsx'),
            ('rinkimai', None),
        ],
        'empty': False,
        'header': ['name', 'title', 'description'],
        'data': [
            [
                {'value': '📄 apygarda', 'link': '/datasets/xlsx/rinkimai/apygarda'},
                {'value': '', 'color': '#f5f5f5'},
                {'value': '', 'color': '#f5f5f5'},
            ],
            [
                {'value': '📄 apylinke', 'link': '/datasets/xlsx/rinkimai/apylinke'},
                {'value': '', 'color': '#f5f5f5'},
                {'value': '', 'color': '#f5f5f5'},
            ],
            [
                {'value': '📄 kandidatas', 'link': '/datasets/xlsx/rinkimai/kandidatas'},
                {'value': '', 'color': '#f5f5f5'},
                {'value': '', 'color': '#f5f5f5'},
            ],
            [
                {'value': '📄 turas', 'link': '/datasets/xlsx/rinkimai/turas'},
                {'value': '', 'color': '#f5f5f5'},
                {'value': '', 'color': '#f5f5f5'},
            ]
        ],
        'formats': [
            ('CSV', '/datasets/xlsx/rinkimai/:ns/:format/csv'),
            ('JSON', '/datasets/xlsx/rinkimai/:ns/:format/json'),
            ('JSONL', '/datasets/xlsx/rinkimai/:ns/:format/jsonl'),
            ('ASCII', '/datasets/xlsx/rinkimai/:ns/:format/ascii'),
        ],
    }


@pytest.mark.models(
    'backends/postgres/report',
    'backends/mongo/report',
)
def test_model(model, context, app):
    app.authmodel(model, ['insert', 'getall'])
    resp = app.post(model, json={'_data': [
        {
            '_op': 'insert',
            '_type': model,
            'status': 'ok',
            'count': 42,
        },
    ]})
    row, = resp.json()['_data']

    resp = app.get(f'/{model}', headers={'accept': 'text/html'})
    assert resp.status_code == 200

    resp.context.pop('request')

    backend = model[len('backends/'):len(model) - len('/report')]
    assert _cleaned_context(resp, data=False) == {
        'location': [
            ('🏠', '/'),
            ('backends', '/backends'),
            (backend, f'/backends/{backend}'),
            ('Report', None),
            ('Changes', f"/{model}/:changes/-10"),
        ],
        'empty': False,
        'header': [
            '_id',
            'report_type',
            'status',
            'valid_from_date',
            'update_time',
            'count',
            'notes[].note',
            'notes[].note_type',
            'notes[].create_date',
            'operating_licenses[].license_types[]',
            'sync.sync_revision',
            'sync.sync_resources[].sync_id',
            'sync.sync_resources[].sync_source',
        ],
        'data': [
            [
                {'value': row['_id'][:8], 'link': f"/{model}/%s" % row['_id']},
                {'value': '', 'color': '#f5f5f5'},
                {'value': 'ok'},
                {'value': '', 'color': '#f5f5f5'},
                {'value': '', 'color': '#f5f5f5'},
                {'value': 42},
                {'value': '', 'color': '#f5f5f5'},
                {'value': '', 'color': '#f5f5f5'},
                {'value': '', 'color': '#f5f5f5'},
                {'value': '', 'color': '#f5f5f5'},
                {'value': '', 'color': '#f5f5f5'},
                {'value': '', 'color': '#f5f5f5'},
                {'value': '', 'color': '#f5f5f5'},
            ],
        ],
        'formats': [
            ('CSV', f'/{model}/:format/csv'),
            ('JSON', f'/{model}/:format/json'),
            ('JSONL', f'/{model}/:format/jsonl'),
            ('ASCII', f'/{model}/:format/ascii'),
        ],
    }


@pytest.mark.models(
    'backends/postgres/report',
    'backends/mongo/report',
)
def test_model_get(model, app):
    app.authmodel(model, ['insert', 'getone'])
    resp = app.post(model, json={'_data': [
        {
            '_op': 'insert',
            '_type': model,
            'status': 'ok',
            'count': 42,
        },
    ]})
    row, = resp.json()['_data']

    _id = row['_id']
    resp = app.get(f'/{model}/{_id}', headers={'accept': 'text/html'})
    assert resp.status_code == 200

    resp.context.pop('request')
    backend = model[len('backends/'):len(model) - len('/report')]
    assert _cleaned_context(resp, data=True) == {
        'location': [
            ('🏠', '/'),
            ('backends', '/backends'),
            (backend, f'/backends/{backend}'),
            ('Report', f'/{model}'),
            (_id[:8], None),
            ('Changes', f'/{model}/%s/:changes/-10' % _id),
        ],
        'header': [
            '_type',
            '_id',
            '_revision',
            'report_type',
            'status',
            'valid_from_date',
            'update_time',
            'count',
            'notes[].note',
            'notes[].note_type',
            'notes[].create_date',
            'operating_licenses[].license_types[]',
            'sync.sync_revision',
            'sync.sync_resources[].sync_id',
            'sync.sync_resources[].sync_source',
        ],
        'data': [
            {
                '_type': model,
                '_id': short_id(_id),
                '_revision': row['_revision'],
                'count': 42,
                'notes[].note': '',
                'notes[].note_type': '',
                'notes[].create_date': '',
                'operating_licenses[].license_types[]': '',
                'report_type': '',
                'status': 'ok',
                'sync.sync_resources[].sync_id': '',
                'sync.sync_resources[].sync_source': '',
                'sync.sync_revision': '',
                'update_time': '',
                'valid_from_date': '',
            }
        ],
        'empty': False,
        'formats': [
            ('CSV', f'/{model}/%s/:format/csv' % row['_id']),
            ('JSON', f'/{model}/%s/:format/json' % row['_id']),
            ('JSONL', f'/{model}/%s/:format/jsonl' % row['_id']),
            ('ASCII', f'/{model}/%s/:format/ascii' % row['_id']),
        ],
    }


def test_dataset(app):
    app.authmodel('datasets/json/rinkimai', ['insert', 'getall'])
    resp = app.post('/datasets/json/rinkimai', json={
        'id': '1',
        'pavadinimas': 'Rinkimai 1',
    })
    data = resp.json()
    assert resp.status_code == 201, data

    resp = app.get('/datasets/json/rinkimai', headers={'accept': 'text/html'})
    assert resp.status_code == 200, resp.json()

    pk = data['_id']
    resp.context.pop('request')
    assert _cleaned_context(resp, data=False) == {
        'location': [
            ('🏠', '/'),
            ('datasets', '/datasets'),
            ('json', '/datasets/json'),
            ('rinkimai', None),
            ('Changes', '/datasets/json/rinkimai/:changes/-10'),
        ],
        'header': ['_id', 'id', 'pavadinimas'],
        'empty': False,
        'data': [
            [
                {'link': f'/datasets/json/rinkimai/{pk}', 'value': pk[:8]},
                {'value': '1'},
                {'value': 'Rinkimai 1'},
            ],
        ],
        'formats': [
            ('CSV', '/datasets/json/rinkimai/:format/csv'),
            ('JSON', '/datasets/json/rinkimai/:format/json'),
            ('JSONL', '/datasets/json/rinkimai/:format/jsonl'),
            ('ASCII', '/datasets/json/rinkimai/:format/ascii'),
        ],
    }


def test_dataset_with_show(context, app):
    app.authmodel('/datasets/json/rinkimai', ['insert', 'search'])

    resp = app.post('/datasets/json/rinkimai', json={
        'id': '1',
        'pavadinimas': 'Rinkimai 1',
    })
    data = resp.json()
    assert resp.status_code == 201, data

    resp = app.get('/datasets/json/rinkimai?select(pavadinimas)', headers={
        'accept': 'text/html',
    })
    assert resp.status_code == 200

    context = _cleaned_context(resp)
    assert context['header'] == ['pavadinimas']
    assert context['data'] == [{
        'pavadinimas': 'Rinkimai 1',
    }]


def test_dataset_url_without_resource(context, app):
    app.authmodel('datasets/json/rinkimai', ['insert', 'getall'])
    resp = app.post('/datasets/json/rinkimai', json={
        'id': '1',
        'pavadinimas': 'Rinkimai 1',
    })
    data = resp.json()
    pk = data['_id']
    assert resp.status_code == 201, data
    resp = app.get('/datasets/json/rinkimai', headers={'accept': 'text/html'})
    assert resp.status_code == 200
    context = _cleaned_context(resp, data=False)
    assert context['header'] == ['_id', 'id', 'pavadinimas']
    assert context['data'] == [[
        {'link': f'/datasets/json/rinkimai/{pk}', 'value': pk[:8]},
        {'value': '1'},
        {'value': 'Rinkimai 1'},
    ]]


def test_nested_dataset(app):
    app.authmodel('datasets/nested/dataset/name/model', ['insert', 'getall'])
    resp = app.post('/datasets/nested/dataset/name/model', json={
        'name': 'Nested One',
    })
    data = resp.json()
    assert resp.status_code == 201, data
    pk = data['_id']

    resp = app.get('/datasets/nested/dataset/name/model', headers={'accept': 'text/html'})
    assert resp.status_code == 200

    resp.context.pop('request')
    assert _cleaned_context(resp, data=False) == {
        'location': [
            ('🏠', '/'),
            ('datasets', '/datasets'),
            ('nested', '/datasets/nested'),
            ('dataset', '/datasets/nested/dataset'),
            ('name', '/datasets/nested/dataset/name'),
            ('model', None),
            ('Changes', '/datasets/nested/dataset/name/model/:changes/-10'),
        ],
        'header': ['_id', 'name'],
        'empty': False,
        'data': [
            [
                {'link': f'/datasets/nested/dataset/name/model/{pk}', 'value': pk[:8]},
                {'value': 'Nested One'},
            ],
        ],
        'formats': [
            ('CSV', '/datasets/nested/dataset/name/model/:format/csv'),
            ('JSON', '/datasets/nested/dataset/name/model/:format/json'),
            ('JSONL', '/datasets/nested/dataset/name/model/:format/jsonl'),
            ('ASCII', '/datasets/nested/dataset/name/model/:format/ascii'),
        ],
    }


def test_dataset_key(app):
    app.authmodel('datasets/json/rinkimai', ['insert', 'getone'])
    resp = app.post('/datasets/json/rinkimai', json={
        'id': '1',
        'pavadinimas': 'Rinkimai 1',
    })
    data = resp.json()
    assert resp.status_code == 201, data
    pk = data['_id']
    rev = data['_revision']

    resp = app.get(f'/datasets/json/rinkimai/{pk}', headers={'accept': 'text/html'})
    assert resp.status_code == 200

    resp.context.pop('request')
    assert _cleaned_context(resp, data=False) == {
        'location': [
            ('🏠', '/'),
            ('datasets', '/datasets'),
            ('json', '/datasets/json'),
            ('rinkimai', '/datasets/json/rinkimai'),
            (short_id(pk), None),
            ('Changes', f'/datasets/json/rinkimai/{pk}/:changes/-10'),
        ],
        'formats': [
            ('CSV', f'/datasets/json/rinkimai/{pk}/:format/csv'),
            ('JSON', f'/datasets/json/rinkimai/{pk}/:format/json'),
            ('JSONL', f'/datasets/json/rinkimai/{pk}/:format/jsonl'),
            ('ASCII', f'/datasets/json/rinkimai/{pk}/:format/ascii'),
        ],
        'header': [
            '_type',
            '_id',
            '_revision',
            'id',
            'pavadinimas',
        ],
        'empty': False,
        'data': [[
            {'value': 'datasets/json/rinkimai'},
            {'value': short_id(pk), 'link': f'/datasets/json/rinkimai/{pk}'},
            {'value': rev},
            {'value': '1'},
            {'value': 'Rinkimai 1'},
        ]],
    }


def test_changes_single_object(app: TestClient, mocker):
    app.authmodel('datasets/json/rinkimai', ['insert', 'patch', 'changes'])

    model = 'datasets/json/rinkimai'

    obj = send(app, model, 'insert', {
        'id': '1',
        'pavadinimas': 'Rinkimai 1',
    })
    obj1 = send(app, model, 'patch', obj, {
        'id': '1',
        'pavadinimas': 'Rinkimai 2',
    })

    resp = app.get(f'/datasets/json/rinkimai/{obj.id}/:changes/-10', headers={
        'accept': 'text/html',
    })
    assert resp.status_code == 200

    change = _cleaned_context(resp)['data']
    assert _cleaned_context(resp, data=False) == {
        'location': [
            ('🏠', '/'),
            ('datasets', '/datasets'),
            ('json', '/datasets/json'),
            ('rinkimai', '/datasets/json/rinkimai'),
            (obj1.sid, f'/datasets/json/rinkimai/{obj.id}'),
            ('Changes', None),
        ],
        'formats': [
            ('CSV', f'/{model}/{obj.id}/:changes/-10/:format/csv?limit(100)'),
            ('JSON', f'/{model}/{obj.id}/:changes/-10/:format/json?limit(100)'),
            ('JSONL', f'/{model}/{obj.id}/:changes/-10/:format/jsonl?limit(100)'),
            ('ASCII', f'/{model}/{obj.id}/:changes/-10/:format/ascii?limit(100)'),
        ],
        'header': [
            '_cid',
            '_created',
            '_op',
            '_id',
            '_txn',
            '_revision',
            'id',
            'pavadinimas',
        ],
        'empty': False,
        'data': [
            [
                {'value': 1},
                {'value': change[0]['_created']},
                {'value': 'insert'},
                {'value': obj.sid, 'link': f'/{model}/{obj.id}'},
                {'value': change[0]['_txn']},
                {'value': obj.rev},
                {'value': '1'},
                {'value': 'Rinkimai 1'},
            ],
            [
                {'value': 2},
                {'value': change[1]['_created']},
                {'value': 'patch'},
                {'value': obj.sid, 'link': f'/datasets/json/rinkimai/{obj.id}'},
                {'value': change[1]['_txn']},
                {'value': obj1.rev},
                {'value': '', 'color': '#f5f5f5'},
                {'value': 'Rinkimai 2'},
            ],
        ],
    }


def test_changes_object_list(app, mocker):
    app.authmodel('datasets/json/rinkimai', ['insert', 'patch', 'changes'])

    model = 'datasets/json/rinkimai'

    obj = send(app, model, 'insert', {
        'id': '1',
        'pavadinimas': 'Rinkimai 1',
    })
    obj1 = send(app, model, 'patch', obj, {
        'id': '1',
        'pavadinimas': 'Rinkimai 2',
    })

    resp = app.get('/datasets/json/rinkimai/:changes/-10', headers={'accept': 'text/html'})
    assert resp.status_code == 200

    change = _cleaned_context(resp)['data']
    assert _cleaned_context(resp, data=False) == {
        'location': [
            ('🏠', '/'),
            ('datasets', '/datasets'),
            ('json', '/datasets/json'),
            ('rinkimai', f'/{model}'),
            ('Changes', None),
        ],
        'formats': [
            ('CSV', f'/{model}/:changes/-10/:format/csv?limit(100)'),
            ('JSON', f'/{model}/:changes/-10/:format/json?limit(100)'),
            ('JSONL', f'/{model}/:changes/-10/:format/jsonl?limit(100)'),
            ('ASCII', f'/{model}/:changes/-10/:format/ascii?limit(100)'),
        ],
        'header': [
            '_cid',
            '_created',
            '_op',
            '_id',
            '_txn',
            '_revision',
            'id',
            'pavadinimas',
        ],
        'empty': False,
        'data': [
            [
                {'value': 1},
                {'value': change[0]['_created']},
                {'value': 'insert'},
                {'link': f'/{model}/{obj.id}', 'value': obj.sid},
                {'value': change[0]['_txn']},
                {'value': obj.rev},
                {'value': '1'},
                {'value': 'Rinkimai 1'},
            ],
            [
                {'value': 2},
                {'value': change[1]['_created']},
                {'value': 'patch'},
                {'link': f'/{model}/{obj.id}', 'value': obj.sid},
                {'value': change[1]['_txn']},
                {'value': obj1.rev},
                {'value': '', 'color': '#f5f5f5'},
                {'value': 'Rinkimai 2'},
            ],
        ],
    }


def test_count(app):
    app.authmodel('/datasets/json/rinkimai', ['upsert', 'search'])

    resp = app.post('/datasets/json/rinkimai', json={'_data': [
        {
            '_op': 'upsert',
            '_type': 'datasets/json/rinkimai',
            '_where': f'id="1"',
            'id': '1',
            'pavadinimas': 'Rinkimai 1',
        },
        {
            '_op': 'upsert',
            '_type': 'datasets/json/rinkimai',
            '_where': f'id="2"',
            'id': '2',
            'pavadinimas': 'Rinkimai 2',
        },
    ]})
    # FIXME: Status code on multiple objects must be 207.
    assert resp.status_code == 200, resp.json()

    resp = app.get('/datasets/json/rinkimai?count()', headers={'accept': 'text/html'})
    assert resp.status_code == 200

    context = _cleaned_context(resp)
    assert context['data'] == [{'count()': 2}]


@pytest.mark.models(
    'backends/postgres/report',
    'backends/mongo/report',
)
def test_post(model, context, app):
    app.authmodel(model, ['insert', 'getone'])

    # tests basic object creation
    resp = app.post(f'/{model}', json={
        'status': 'ok',
        'count': 42,
    })
    assert resp.status_code == 201
    data = resp.json()
    id_ = data['_id']
    revision = data['_revision']
    assert uuid.UUID(id_).version == 4
    assert data == {
        '_type': model,
        '_id': id_,
        '_revision': revision,
        'report_type': None,
        'status': 'ok',
        'valid_from_date': None,
        'update_time': None,
        'count': 42,
        'notes': [],
        'operating_licenses': [],
        'sync': {
            'sync_revision': None,
            'sync_resources': [],
        },
    }

    resp = app.get(f'/{model}/{id_}')
    assert resp.status_code == 200
    assert resp.json() == {
        '_type': model,
        '_id': id_,
        '_revision': revision,
        'report_type': None,
        'status': 'ok',
        'valid_from_date': None,
        'update_time': None,
        'count': 42,
        'notes': [],
        'operating_licenses': [],
        'sync': {
            'sync_revision': None,
            'sync_resources': [],
        },
    }


@pytest.mark.models(
    'backends/postgres/report',
    'backends/mongo/report',
)
def test_post_invalid_json(model, context, app):
    # tests 400 response on invalid json
    app.authmodel(model, ['insert'])
    headers = {"content-type": "application/json"}
    resp = app.post(f'/{model}', headers=headers, content="""{
        "status": "ok",
        "count": 42
    ]""")
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["JSONError"]


@pytest.mark.models(
    'backends/postgres/report',
    'backends/mongo/report',
)
def test_post_empty_content(model, context, app):
    # tests posting empty content
    app.authmodel(model, ['insert'])
    headers = {
        "content-length": "0",
        "content-type": "application/json",
    }
    resp = app.post(f'/{model}', headers=headers, json=None)
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["JSONError"]


@pytest.mark.models(
    'backends/postgres/report',
    'backends/mongo/report',
)
def test_post_id(model, context, app):
    # tests 400 response when trying to create object with id
    app.authmodel(model, ['insert'])

    # XXX: there's a funny thing that id value is loaded/validated first
    # before it's checked that user has correct scope
    resp = app.post(f'/{model}', json={
        '_id': '0007ddec-092b-44b5-9651-76884e6081b4',
        'status': 'ok',
        'count': 42,
    })
    assert resp.status_code == 403
    assert get_error_codes(resp.json()) == ["InsufficientScopeError"]
    assert 'www-authenticate' in resp.headers
    assert resp.headers['www-authenticate'] == 'Bearer error="insufficient_scope"'


@pytest.mark.models(
    'backends/postgres/report',
    'backends/mongo/report',
)
def test_insufficient_scope(model, context, app):
    # tests 400 response when trying to create object with id
    app.authmodel(model, ['getone'])

    # XXX: there's a funny thing that id value is loaded/validated first
    # before it's checked that user has correct scope
    resp = app.post(f'/{model}', json={
        'status': 'ok',
        'count': 42,
    })
    assert resp.status_code == 403
    assert get_error_codes(resp.json()) == ["InsufficientScopeError"]
    assert 'www-authenticate' in resp.headers
    assert resp.headers['www-authenticate'] == 'Bearer error="insufficient_scope"'


@pytest.mark.models(
    'backends/postgres/report',
    'backends/mongo/report',
)
def test_post_update_postgres(model, context, app):
    # tests if update works with `id` present in the json
    app.authorize(['spinta_set_meta_fields'])
    app.authmodel(model, ['insert'])
    resp = app.post(f'/{model}', json={
        '_id': '0007ddec-092b-44b5-9651-76884e6081b4',
        'status': 'ok',
        'count': 42,
    })

    assert resp.status_code == 201
    assert resp.json()['_id'] == '0007ddec-092b-44b5-9651-76884e6081b4'

    # POST invalid id
    resp = app.post(f'/{model}', json={
        '_id': 123,
        'status': 'not-ok',
        'count': 13,
    })

    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]

    # POST invalid id
    resp = app.post(f'/{model}', json={
        '_id': '123',
        'status': 'not-ok',
        'count': 13,
    })

    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]

    # POST invalid id
    resp = app.post(f'/{model}', json={
        '_id': None,
        'status': 'not-ok',
        'count': 13,
    })

    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]


@pytest.mark.models(
    'backends/postgres/report',
    'backends/mongo/report',
)
def test_post_revision(model, context, app):
    # tests 400 response when trying to create object with revision
    app.authmodel(model, ['insert'])
    resp = app.post(f"/{model}", json={
        '_revision': 'r3v1510n',
        'status': 'valid',
        'count': 42,
    })
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["ManagedProperty"]


@pytest.mark.models(
    'backends/postgres/report',
    'backends/mongo/report',
)
def test_post_duplicate_id(model, app):
    # tests 400 response when trying to create object with id which exists
    app.authorize(['spinta_set_meta_fields'])
    app.authmodel(model, ['insert'])
    resp = app.post(f'/{model}', json={
        'status': 'valid',
        'count': 42,
    })
    assert resp.status_code == 201
    data = resp.json()
    id_ = data['_id']

    resp = app.post(f'/{model}', json={
        '_id': id_,
        'status': 'valid',
        'count': 42,
    })
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["UniqueConstraint"]


@pytest.mark.models(
    'backends/postgres/report',
    'backends/mongo/report',
)
def test_patch_duplicate_id(model, context, app):
    # tests that duplicate ID detection works with PATCH requests
    app.authmodel(model, ['insert', 'getone', 'patch'])
    app.authorize(['spinta_set_meta_fields'])

    # create extra report
    resp = app.post(f'/{model}', json={
        '_type': 'report',
        'status': '1',
    })
    assert resp.status_code == 201

    data = app.post(f'/{model}', json={
        '_type': 'report',
        'status': '1',
    }).json()
    id_ = data['_id']
    new_id = '0007ddec-092b-44b5-9651-76884e6081b4'
    # try to PATCH id with set_meta_fields scope
    # this should be successful because id that we want to setup
    # does not exist in database
    resp = app.patch(f'/{model}/{id_}', json={
        '_id': new_id,
        '_revision': data['_revision'],
    })
    assert resp.status_code == 200
    data = resp.json()
    id_ = data['_id']
    assert id_ == new_id

    # try to patch report with id of itself
    resp = app.patch(f'/{model}/{id_}', json={
        '_id': new_id,
        '_revision': data['_revision'],
    })
    assert resp.status_code == 200
    assert resp.json()['_id'] == new_id

    # try to PATCH id with set_meta_fields scope
    # this should not be successful because id that we want to setup
    # already exists in database
    resp = app.post(f'/{model}', json={
        '_type': 'report',
        'status': '1',
    })
    existing_id = resp.json()['_id']

    resp = app.patch(f'/{model}/{id_}', json={
        '_id': existing_id,
        '_revision': data['_revision'],
    })
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ['UniqueConstraint']


@pytest.mark.models(
    'backends/postgres/report',
    'backends/mongo/report',
)
def test_post_non_json_content_type(model, app):
    # tests 400 response when trying to make non-json request
    app.authmodel(model, ['insert'])
    headers = {"content-type": "application/text"}
    resp = app.post(f'/{model}', headers=headers, json={
        'status': 'valid',
        'count': 42,
    })
    assert resp.status_code == 415
    assert get_error_codes(resp.json()) == ["UnknownContentType"]


@pytest.mark.models(
    'backends/postgres/report',
    'backends/mongo/report',
)
def test_post_bad_auth_header(model, app):
    # tests 400 response when authorization header is missing `Bearer `
    auth_header = {'authorization': 'Fail f00b4rb4z'}
    resp = app.post(f'/{model}', headers=auth_header, json={
        'status': 'valid',
        'count': 42,
    })
    assert resp.status_code == 401
    assert get_error_codes(resp.json()) == ["UnsupportedTokenTypeError"]


@pytest.mark.models(
    'backends/postgres/report',
    'backends/mongo/report',
)
def test_post_missing_auth_header(model, context, app, mocker):
    mocker.patch.object(context.get('config'), 'default_auth_client', None)
    resp = app.post(f'/{model}', json={
        'status': 'valid',
        'count': 42,
    })
    assert resp.status_code == 401
    assert get_error_codes(resp.json()) == ["MissingAuthorizationError"]


@pytest.mark.models(
    'backends/postgres/report',
    'backends/mongo/report',
)
def test_post_invalid_report_schema(model, app):
    # tests validation of correct value types according to manifest's schema
    app.authmodel(model, ['insert', 'getone'])

    # test integer validation
    resp = app.post(f'/{model}', json={
        'count': '123',
    })
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]
    assert get_error_context(
        resp.json(),
        "InvalidValue",
        ["manifest", "model", "property", "type"],
    ) == {
        'manifest': 'default',
        'model': model,
        'property': 'count',
        'type': 'integer',
    }

    resp = app.post(f'/{model}', json={
        'count': False,
    })
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]

    resp = app.post(f'/{model}', json={
        'count': [1, 2, 3],
    })
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]

    resp = app.post(f'/{model}', json={
        'count': {'a': 1, 'b': 2},
    })
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]

    resp = app.post(f'/{model}', json={
        'count': 123,
    })
    assert resp.status_code == 201

    # sanity check, that integers are still allowed.
    resp = app.post(f'/{model}', json={
        'status': 42,
    })
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]

    # test string validation
    resp = app.post(f'/{model}', json={
        'status': True,
    })
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]

    resp = app.post(f'/{model}', json={
        'status': [1, 2, 3],
    })
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]

    resp = app.post(f'/{model}', json={
        'status': {'a': 1, 'b': 2},
    })
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]

    # sanity check, that strings are still allowed.
    resp = app.post(f'/{model}', json={
        'status': '42',
    })
    assert resp.status_code == 201


@pytest.mark.models(
    'backends/postgres/report',
    'backends/mongo/report',
)
def test_streaming_response(model, app):
    app.authmodel(model, ['insert', 'getall'])
    resp = app.post(f'/{model}', json={'_data': [
        {
            '_op': 'insert',
            '_type': model,
            'count': 42,
            'status': 'ok',
        },
        {
            '_op': 'insert',
            '_type': model,
            'count': 13,
            'status': 'not-ok',
        },
    ]})
    # FIXME: Should be 207 status code.
    assert resp.status_code == 200, resp.json()

    resp = app.get(f'/{model}').json()
    data = resp['_data']
    data = sorted((x['count'], x['status']) for x in data)
    assert data == [
        (13, 'not-ok'),
        (42, 'ok'),
    ]


@pytest.mark.models(
    'backends/postgres/report',
    'backends/mongo/report',
)
def test_multi_backends(model, app):
    app.authmodel(model, ['insert', 'getone', 'getall', 'search'])
    resp = app.post(f'/{model}', json={'status': '42'})
    assert resp.status_code == 201
    data = resp.json()
    id_ = data['_id']
    revision = data['_revision']

    # return the object on GETONE
    resp = app.get(f'/{model}/{id_}')
    assert resp.status_code == 200
    data = resp.json()
    assert data['_id'] == id_
    assert data['_revision'] == revision
    assert data['_type'] == model
    assert data['status'] == '42'

    # check that query parameters work on GETONE
    resp = app.get(f'/{model}/{id_}?select(status)')
    assert resp.status_code == 200
    assert resp.json() == {
        'status': '42',
    }

    # return the objects on GETALL
    resp = app.get(f'/{model}')
    assert resp.status_code == 200
    assert len(resp.json()['_data']) == 1
    data = resp.json()['_data'][0]
    assert data['_id'] == id_
    assert data['_revision'] == revision
    assert data['_type'] == model
    assert data['status'] == '42'

    # check that query parameters work on GETALL
    resp = app.get(f'/{model}?select(status)')
    assert resp.status_code == 200
    assert resp.json()['_data'] == [{
        'status': '42',
    }]


@pytest.mark.models(
    'backends/postgres/report',
    'backends/mongo/report',
)
def test_location_header(model, app, context):
    app.authmodel(model, ['insert'])
    resp = app.post(f'/{model}', json={'status': '42'})
    assert resp.status_code == 201
    assert 'location' in resp.headers
    id_ = resp.json()['_id']
    server_url = context.get('config').server_url
    assert resp.headers['location'] == f'{server_url}{model}s/{id_}'


@pytest.mark.models(
    'backends/postgres/report',
    'backends/mongo/report',
)
def test_upsert_where_ast(model, app):
    app.authmodel(model, ['upsert', 'changes'])
    resp = app.post('/' + model, json={
        '_op': 'upsert',
        '_where': {
            'name': 'eq',
            'args': [
                {'name': 'bind', 'args': ['status']},
                'ok',
            ],
        },
        'status': 'ok',
    })
    data = resp.json()
    assert resp.status_code == 201, data


@pytest.mark.models(
    'backends/postgres/report',
    'backends/mongo/report',
)
def test_upsert_where_ast(model, app):
    app.authmodel(model, ['upsert', 'changes'])

    payload = {
        '_op': 'upsert',
        '_where': {
            'name': 'eq',
            'args': [
                {'name': 'bind', 'args': ['status']},
                'ok',
            ],
        },
        'status': 'ok',
        'valid_from_date': '2021-03-31',
    }

    resp = app.post('/' + model, json=payload)
    data = resp.json()
    assert resp.status_code == 201, data
    rev = data['_revision']

    resp = app.post('/' + model, json=payload)
    data = resp.json()
    assert resp.status_code == 200, data
    assert data['_revision'] == rev


def test_robots(app: TestClient):
    resp = app.get('/robots.txt')
    assert resp.status_code == 200
    assert resp.text == 'User-Agent: *\nAllow: /\n'


@pytest.mark.parametrize('path', [
    'example/City',
    'example/City/19e4f199-93c5-40e5-b04e-a575e81ac373',
    'example/City/:changes',
    'example/City/19e4f199-93c5-40e5-b04e-a575e81ac373/:changes',
])
def test_head_method(
    rc: RawConfig,
    path: str,
):
    context, manifest = prepare_manifest(rc, '''
    d | r | b | m | property | type   | ref  | access
    example                  |        |      |
      |   |   | City         |        | name |
      |   |   |   | name     | string |      | open
    ''', backend='memory')

    context.loaded = True

    db: Memory = manifest.backend
    db.insert({
        '_type': 'example/City',
        '_id': '19e4f199-93c5-40e5-b04e-a575e81ac373',
        '_revision': 'b6197bb7-3592-4cdb-a61c-5a618f44950c',
        'name': 'Vilnius',
    })

    app = create_test_client(context, scope=[
        'spinta_getall',
        'spinta_search',
        'spinta_getone',
        'spinta_changes',
    ])

    resp = app.head(f'/{path}')
    assert resp.status_code == 200
    assert resp.content == b''
