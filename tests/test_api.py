import uuid

import pytest

from spinta.testing.utils import get_error_codes, get_error_context
from spinta.utils.nestedstruct import flatten


def _cleaned_context(resp):
    data = resp.context.copy()
    data['data'] = [
        {k: v['value'] for k, v in zip(resp.context['header'], d)}
        for d in resp.context['data']
    ]
    return data


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
            ('root', '/'),
        ],
        'header': ['_type', '_id', 'title'],
        'data': data['data'],
        'row': [],
        'formats': [
            ('CSV', '/:format/csv'),
            ('JSON', '/:format/json'),
            ('JSONL', '/:format/jsonl'),
            ('ASCII', '/:format/ascii'),
        ],
        'limit_enforced': True,
    }
    assert next(d for d in data['data'] if d['_id'] == 'report') == {
        '_type': 'model',
        '_id': 'report',
        'title': 'Report',
    }


def test_directory(app):
    app.authorize(['spinta_datasets_getall'])
    resp = app.get('/datasets/xlsx/rinkimai/:ns', headers={'accept': 'text/html'})
    assert resp.status_code == 200

    resp.context.pop('request')
    assert _cleaned_context(resp) == {
        'location': [
            ('root', '/'),
            ('datasets', '/datasets'),
            ('xlsx', '/datasets/xlsx'),
            ('rinkimai', None),
        ],
        'header': ['_type', '_id', 'title'],
        'data': [
            {'_type': 'model', '_id': 'datasets/xlsx/rinkimai/apygarda', 'title': ''},
            {'_type': 'model', '_id': 'datasets/xlsx/rinkimai/apylinke', 'title': ''},
            {'_type': 'model', '_id': 'datasets/xlsx/rinkimai/kandidatas', 'title': ''},
            {'_type': 'model', '_id': 'datasets/xlsx/rinkimai/turas', 'title': ''},
        ],
        'row': [],
        'formats': [
            ('CSV', '/datasets/xlsx/rinkimai/:ns/:format/csv'),
            ('JSON', '/datasets/xlsx/rinkimai/:ns/:format/json'),
            ('JSONL', '/datasets/xlsx/rinkimai/:ns/:format/jsonl'),
            ('ASCII', '/datasets/xlsx/rinkimai/:ns/:format/ascii'),
        ],
        'limit_enforced': True,
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
    assert resp.context == {
        'location': [
            ('root', '/'),
            ('backends', '/backends'),
            (backend, f'/backends/{backend}'),
            ('report', None),
            (':changes', f"/{model}/:changes"),
        ],
        'header': [
            '_id',
            'report_type',
            'status',
            'valid_from_date',
            'update_time',
            'count',
        ],
        'data': [
            [
                {'color': None, 'link': f"/{model}/%s" % row['_id'], 'value': row['_id'][:8]},
                {'color': '#C1C1C1', 'link': None, 'value': ''},
                {'color': None, 'link': None, 'value': 'ok'},
                {'color': '#C1C1C1', 'link': None, 'value': ''},
                {'color': '#C1C1C1', 'link': None, 'value': ''},
                {'color': None, 'link': None, 'value': 42},
            ],
        ],
        'row': [],
        'formats': [
            ('CSV', f'/{model}/:format/csv'),
            ('JSON', f'/{model}/:format/json'),
            ('JSONL', f'/{model}/:format/jsonl'),
            ('ASCII', f'/{model}/:format/ascii'),
        ],
        'limit_enforced': True,
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

    resp = app.get(f'/{model}/%s' % row['_id'], headers={'accept': 'text/html'})
    assert resp.status_code == 200

    resp.context.pop('request')
    backend = model[len('backends/'):len(model) - len('/report')]
    assert resp.context == {
        'location': [
            ('root', '/'),
            ('backends', '/backends'),
            (backend, f'/backends/{backend}'),
            ('report', f'/{model}'),
            (row['_id'][:8], None),
            (':changes', f'/{model}/%s/:changes' % row['_id']),
        ],
        'header': [],
        'data': [],
        'row': [
            ('_type', {'color': None, 'link': None, 'value': model}),
            ('_id', {'color': None, 'link': f'/{model}/%s' % row['_id'], 'value': row['_id']}),
            ('_revision', {'color': None, 'link': None, 'value': row['_revision']}),
            ('report_type', {'color': '#C1C1C1', 'link': None, 'value': ''}),
            ('status', {'color': None, 'link': None, 'value': 'ok'}),
            ('valid_from_date', {'color': '#C1C1C1', 'link': None, 'value': ''}),
            ('update_time', {'color': '#C1C1C1', 'link': None, 'value': ''}),
            ('count', {'color': None, 'link': None, 'value': 42}),
        ],
        'formats': [
            ('CSV', f'/{model}/%s/:format/csv' % row['_id']),
            ('JSON', f'/{model}/%s/:format/json' % row['_id']),
            ('JSONL', f'/{model}/%s/:format/jsonl' % row['_id']),
            ('ASCII', f'/{model}/%s/:format/ascii' % row['_id']),
        ],
        'limit_enforced': False,
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
    assert resp.context == {
        'location': [
            ('root', '/'),
            ('datasets', '/datasets'),
            ('json', '/datasets/json'),
            ('rinkimai', None),
            (':changes', '/datasets/json/rinkimai/:changes'),
        ],
        'header': ['_id', 'id', 'pavadinimas'],
        'data': [
            [
                {'color': None, 'link': f'/datasets/json/rinkimai/{pk}', 'value': pk[:8]},
                {'color': None, 'link': None, 'value': '1'},
                {'color': None, 'link': None, 'value': 'Rinkimai 1'},
            ],
        ],
        'row': [],
        'formats': [
            ('CSV', '/datasets/json/rinkimai/:format/csv'),
            ('JSON', '/datasets/json/rinkimai/:format/json'),
            ('JSONL', '/datasets/json/rinkimai/:format/jsonl'),
            ('ASCII', '/datasets/json/rinkimai/:format/ascii'),
        ],
        'limit_enforced': True,
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

    resp.context.pop('request')
    assert resp.context['header'] == ['pavadinimas']
    assert [tuple(y['value'] for y in x) for x in resp.context['data']] == [
        ('Rinkimai 1',),
    ]


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
    assert resp.context['header'] == ['_id', 'id', 'pavadinimas']
    assert resp.context['data'] == [[
        {'color': None, 'link': f'/datasets/json/rinkimai/{pk}', 'value': pk[:8]},
        {'color': None, 'link': None, 'value': '1'},
        {'color': None, 'link': None, 'value': 'Rinkimai 1'},
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
    assert resp.context == {
        'location': [
            ('root', '/'),
            ('datasets', '/datasets'),
            ('nested', '/datasets/nested'),
            ('dataset', '/datasets/nested/dataset'),
            ('name', '/datasets/nested/dataset/name'),
            ('model', None),
            (':changes', '/datasets/nested/dataset/name/model/:changes'),
        ],
        'header': ['_id', 'name'],
        'data': [
            [
                {'color': None, 'link': f'/datasets/nested/dataset/name/model/{pk}', 'value': pk[:8]},
                {'color': None, 'link': None, 'value': 'Nested One'},
            ],
        ],
        'row': [],
        'formats': [
            ('CSV', '/datasets/nested/dataset/name/model/:format/csv'),
            ('JSON', '/datasets/nested/dataset/name/model/:format/json'),
            ('JSONL', '/datasets/nested/dataset/name/model/:format/jsonl'),
            ('ASCII', '/datasets/nested/dataset/name/model/:format/ascii'),
        ],
        'limit_enforced': True,
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
    assert resp.context == {
        'location': [
            ('root', '/'),
            ('datasets', '/datasets'),
            ('json', '/datasets/json'),
            ('rinkimai', '/datasets/json/rinkimai'),
            (pk[:8], None),
            (':changes', f'/datasets/json/rinkimai/{pk}/:changes'),
        ],
        'formats': [
            ('CSV', f'/datasets/json/rinkimai/{pk}/:format/csv'),
            ('JSON', f'/datasets/json/rinkimai/{pk}/:format/json'),
            ('JSONL', f'/datasets/json/rinkimai/{pk}/:format/jsonl'),
            ('ASCII', f'/datasets/json/rinkimai/{pk}/:format/ascii'),
        ],
        'header': [],
        'data': [],
        'row': [
            ('_type', {'color': None, 'link': None, 'value': 'datasets/json/rinkimai'}),
            ('_id', {'color': None, 'link': f'/datasets/json/rinkimai/{pk}', 'value': pk}),
            ('_revision', {'color': None, 'link': None, 'value': rev}),
            ('id', {'color': None, 'link': None, 'value': '1'}),
            ('pavadinimas', {'color': None, 'link': None, 'value': 'Rinkimai 1'}),
        ],
        'limit_enforced': False,
    }


def test_changes_single_object(app, mocker):
    app.authmodel('datasets/json/rinkimai', ['upsert', 'changes'])
    resp = app.post('/datasets/json/rinkimai', json={
        '_op': 'upsert',
        '_where': f'id="1"',
        'id': '1',
        'pavadinimas': 'Rinkimai 1',
    })
    data = resp.json()
    assert resp.status_code == 201, data
    pk = data['_id']
    rev1 = data['_revision']
    resp = app.post('/datasets/json/rinkimai', json={
        '_op': 'upsert',
        '_where': f'id="1"',
        'id': '1',
        'pavadinimas': 'Rinkimai 2',
    })
    assert resp.status_code == 200, resp.json()
    data = resp.json()
    rev2 = data['_revision']
    assert rev1 != rev2
    assert data == {
        '_id': pk,
        '_revision': rev2,
        '_type': 'datasets/json/rinkimai',
        'pavadinimas': 'Rinkimai 2',
    }

    resp = app.get(f'/datasets/json/rinkimai/{pk}/:changes', headers={'accept': 'text/html'})
    assert resp.status_code == 200

    resp.context.pop('request')
    change = [
        {
            k: v['value']
            for k, v in zip(resp.context['header'], d)
        }
        for d in resp.context['data']
    ]
    assert resp.context == {
        'location': [
            ('root', '/'),
            ('datasets', '/datasets'),
            ('json', '/datasets/json'),
            ('rinkimai', '/datasets/json/rinkimai'),
            (pk[:8], f'/datasets/json/rinkimai/{pk}'),
            (':changes', None),
        ],
        'formats': [
            ('CSV', f'/datasets/json/rinkimai/{pk}/:changes/:format/csv'),
            ('JSON', f'/datasets/json/rinkimai/{pk}/:changes/:format/json'),
            ('JSONL', f'/datasets/json/rinkimai/{pk}/:changes/:format/jsonl'),
            ('ASCII', f'/datasets/json/rinkimai/{pk}/:changes/:format/ascii'),
        ],
        'header': [
            '_id',
            '_revision',
            '_txn',
            '_created',
            '_op',
            '_rid',
            'id',
            'pavadinimas',
        ],
        'data': [
            [
                {'color': None, 'link': None, 'value': change[0]['_id']},
                {'color': None, 'link': None, 'value': rev2},
                {'color': None, 'link': None, 'value': change[0]['_txn']},
                {'color': None, 'link': None, 'value': change[0]['_created']},
                {'color': None, 'link': None, 'value': 'upsert'},
                {'color': None, 'link': f'/datasets/json/rinkimai/{pk}', 'value': pk[:8]},
                {'color': '#C1C1C1', 'link': None, 'value': '1'},
                {'color': '#B2E2AD', 'link': None, 'value': 'Rinkimai 2'},
            ],
            [
                {'color': None, 'link': None, 'value': change[1]['_id']},
                {'color': None, 'link': None, 'value': rev1},
                {'color': None, 'link': None, 'value': change[1]['_txn']},
                {'color': None, 'link': None, 'value': change[1]['_created']},
                {'color': None, 'link': None, 'value': 'upsert'},
                {'color': None, 'link': f'/datasets/json/rinkimai/{pk}', 'value': pk[:8]},
                {'color': '#B2E2AD', 'link': None, 'value': '1'},
                {'color': '#B2E2AD', 'link': None, 'value': 'Rinkimai 1'},
            ],
        ],
        'row': [],
        'limit_enforced': True,
    }


def test_changes_object_list(app, mocker):
    app.authmodel('datasets/json/rinkimai', ['upsert', 'changes'])

    resp = app.post('/datasets/json/rinkimai', json={
        '_op': 'upsert',
        '_where': f'id="1"',
        'id': '1',
        'pavadinimas': 'Rinkimai 1',
    })
    data = resp.json()
    assert resp.status_code == 201, data
    pk = data['_id']
    rev1 = data['_revision']
    resp = app.post('/datasets/json/rinkimai', json={
        '_op': 'upsert',
        '_where': f'id="1"',
        'id': '1',
        'pavadinimas': 'Rinkimai 2',
    })
    data = resp.json()
    assert resp.status_code == 200, data
    rev2 = data['_revision']
    assert rev1 != rev2

    resp = app.get('/datasets/json/rinkimai/:changes', headers={'accept': 'text/html'})
    assert resp.status_code == 200

    resp.context.pop('request')
    change = [
        {
            k: v['value']
            for k, v in zip(resp.context['header'], d)
        }
        for d in resp.context['data']
    ]
    assert resp.context == {
        'location': [
            ('root', '/'),
            ('datasets', '/datasets'),
            ('json', '/datasets/json'),
            ('rinkimai', '/datasets/json/rinkimai'),
            (':changes', None),
        ],
        'formats': [
            ('CSV', '/datasets/json/rinkimai/:changes/:format/csv'),
            ('JSON', '/datasets/json/rinkimai/:changes/:format/json'),
            ('JSONL', '/datasets/json/rinkimai/:changes/:format/jsonl'),
            ('ASCII', '/datasets/json/rinkimai/:changes/:format/ascii'),
        ],
        'header': [
            '_id',
            '_revision',
            '_txn',
            '_created',
            '_op',
            '_rid',
            'id',
            'pavadinimas',
        ],
        'data': [
            [
                {'color': None, 'link': None, 'value': change[0]['_id']},
                {'color': None, 'link': None, 'value': rev2},
                {'color': None, 'link': None, 'value': change[0]['_txn']},
                {'color': None, 'link': None, 'value': change[0]['_created']},
                {'color': None, 'link': None, 'value': 'upsert'},
                {'color': None, 'link': f'/datasets/json/rinkimai/{pk}', 'value': pk[:8]},
                {'color': '#C1C1C1', 'link': None, 'value': '1'},
                {'color': '#B2E2AD', 'link': None, 'value': 'Rinkimai 2'},
            ],
            [
                {'color': None, 'link': None, 'value': change[1]['_id']},
                {'color': None, 'link': None, 'value': rev1},
                {'color': None, 'link': None, 'value': change[1]['_txn']},
                {'color': None, 'link': None, 'value': change[1]['_created']},
                {'color': None, 'link': None, 'value': 'upsert'},
                {'color': None, 'link': f'/datasets/json/rinkimai/{pk}', 'value': pk[:8]},
                {'color': '#B2E2AD', 'link': None, 'value': '1'},
                {'color': '#B2E2AD', 'link': None, 'value': 'Rinkimai 1'},
            ],
        ],
        'row': [],
        'limit_enforced': True,
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

    resp.context.pop('request')
    assert resp.context['header'] == ['count()']
    assert resp.context['data'] == [[{'color': None, 'link': None, 'value': 2}]]


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
    resp = app.post(f'/{model}', headers=headers, data="""{
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
