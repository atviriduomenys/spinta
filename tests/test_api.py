import uuid
import datetime

import pytest

from spinta.testing.utils import get_error_codes, get_error_context, get_model_scopes
from spinta.utils.nestedstruct import flatten
from spinta.utils.itertools import consume
from spinta.utils.refs import get_ref_id


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
    assert sorted(flatten(resp.json())) == [
        'api.version',
        'build',
        'implementation.name',
        'implementation.version',
    ]


def test_app(app):
    resp = app.get('/', headers={'accept': 'text/html'})
    assert resp.status_code == 200

    resp.context.pop('request')
    assert _cleaned_context(resp) == {
        'location': [
            ('root', '/'),
        ],
        'header': ['id', 'type', 'name', 'specifier', 'title'],
        'data': [
            {'type': 'ns', 'specifier': ':ns', 'id': 'backends/:ns', 'name': 'backends', 'title': 'backends'},
            {'type': 'ns', 'specifier': ':ns', 'id': 'capital/:ns', 'name': 'capital', 'title': 'capital'},
            {'type': 'ns', 'specifier': ':ns', 'id': 'continent/:ns', 'name': 'continent', 'title': 'continent'},
            {'type': 'ns', 'specifier': ':ns', 'id': 'country/:ns', 'name': 'country', 'title': 'country'},
            {'type': 'ns', 'specifier': ':ns', 'id': 'deeply/:ns', 'name': 'deeply', 'title': 'deeply'},
            {'type': 'ns', 'specifier': ':ns', 'id': 'nested/:ns', 'name': 'nested', 'title': 'nested'},
            {'type': 'ns', 'specifier': ':ns', 'id': 'org/:ns', 'name': 'org', 'title': 'org'},
            {'type': 'ns', 'specifier': ':ns', 'id': 'photo/:ns', 'name': 'photo', 'title': 'photo'},
            {'type': 'ns', 'specifier': ':ns', 'id': 'report/:ns', 'name': 'report', 'title': 'report'},
            {'type': 'ns', 'specifier': ':ns', 'id': 'rinkimai/:ns', 'name': 'rinkimai', 'title': 'rinkimai'},
            {'type': 'ns', 'specifier': ':ns', 'id': 'tenure/:ns', 'name': 'tenure', 'title': 'tenure'},
        ],
        'row': [],
        'formats': [
            ('CSV', '/:format/csv'),
            ('JSON', '/:format/json'),
            ('JSONL', '/:format/jsonl'),
            ('ASCII', '/:format/ascii'),
        ],
    }


def test_directory(app):
    resp = app.get('/rinkimai', headers={'accept': 'text/html'})
    assert resp.status_code == 200

    resp.context.pop('request')
    assert _cleaned_context(resp) == {
        'location': [
            ('root', '/'),
            ('rinkimai', None),
        ],
        'header': ['id', 'type', 'name', 'specifier', 'title'],
        'data': [
            {'type': 'ns', 'specifier': ':ns', 'id': 'rinkimai/apygarda/:ns', 'name': 'rinkimai/apygarda', 'title': 'apygarda'},
            {'type': 'ns', 'specifier': ':ns', 'id': 'rinkimai/apylinke/:ns', 'name': 'rinkimai/apylinke', 'title': 'apylinke'},
            {'type': 'ns', 'specifier': ':ns', 'id': 'rinkimai/kandidatas/:ns', 'name': 'rinkimai/kandidatas', 'title': 'kandidatas'},
            {'type': 'ns', 'specifier': ':ns', 'id': 'rinkimai/turas/:ns', 'name': 'rinkimai/turas', 'title': 'turas'},
            {'type': 'model:dataset', 'specifier': ':dataset/json/:resource/data', 'id': 'rinkimai/:dataset/json/:resource/data', 'name': 'rinkimai', 'title': ''},
            {'type': 'model:dataset', 'specifier': ':dataset/xlsx/:resource/data', 'id': 'rinkimai/:dataset/xlsx/:resource/data', 'name': 'rinkimai', 'title': ''},
        ],
        'row': [],
        'formats': [
            ('CSV', '/rinkimai/:format/csv'),
            ('JSON', '/rinkimai/:format/json'),
            ('JSONL', '/rinkimai/:format/jsonl'),
            ('ASCII', '/rinkimai/:format/ascii'),
        ],
    }


@pytest.mark.models(
    'backends/postgres/report',
    'backends/mongo/report',
)
def test_model(model, context, app):
    row, = context.push([
        {
            "type": model,
            "status": "ok",
            "count": 42,
        },
    ])

    app.authmodel(model, ["getall"])
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
            'id',
            'report_type',
            'status',
            'valid_from_date',
            'update_time',
            'count',
            'notes',
            'operating_licenses',
            'pdf',
        ],
        'data': [
            [
                {'color': None, 'link': f"/{model}/%s" % row['id'], 'value': row['id'][:8]},
                {'color': '#C1C1C1', 'link': None, 'value': ''},
                {'color': None, 'link': None, 'value': 'ok'},
                {'color': '#C1C1C1', 'link': None, 'value': ''},
                {'color': '#C1C1C1', 'link': None, 'value': ''},
                {'color': None, 'link': None, 'value': 42},
                {'color': None, 'link': None, 'value': []},
                {'color': None, 'link': None, 'value': []},
                {'color': '#C1C1C1', 'link': None, 'value': ''},  # XXX: this is pdf value which should be hidden?
            ],
        ],
        'row': [],
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
def test_model_get(model, context, app):
    row, = context.push([
        {
            "type": model,
            "status": "ok",
            "count": 42,
        },
    ])

    app.authmodel(model, ["getone"])
    resp = app.get(f'/{model}/%s' % row['id'], headers={'accept': 'text/html'})
    assert resp.status_code == 200

    resp.context.pop('request')
    revision = resp.context['row'][1][1]['value']
    backend = model[len('backends/'):len(model) - len('/report')]
    assert resp.context == {
        'location': [
            ('root', '/'),
            ('backends', '/backends'),
            (backend, f'/backends/{backend}'),
            ('report', f'/{model}'),
            (row['id'][:8], None),
            (':changes', f'/{model}/%s/:changes' % row['id']),
        ],
        'header': [],
        'data': [],
        'row': [
            ('id', {'color': None, 'link': f'/{model}/%s' % row['id'], 'value': row['id']}),
            ('revision', {'color': None, 'link': None, 'value': revision}),
            ('report_type', {'color': '#C1C1C1', 'link': None, 'value': ''}),
            ('status', {'color': None, 'link': None, 'value': 'ok'}),
            ('valid_from_date', {'color': '#C1C1C1', 'link': None, 'value': ''}),
            ('update_time', {'color': '#C1C1C1', 'link': None, 'value': ''}),
            ('count', {'color': None, 'link': None, 'value': 42}),
            ('notes', {'color': None, 'link': None, 'value': []}),
            ('operating_licenses', {'color': None, 'link': None, 'value': []}),
            ('pdf', {'color': '#C1C1C1', 'link': None, 'value': ''}),  # XXX: this is pdf value which should be hidden?
            ('type', {'color': None, 'link': None, 'value': model}),
        ],
        'formats': [
            ('CSV', f'/{model}/%s/:format/csv' % row['id']),
            ('JSON', f'/{model}/%s/:format/json' % row['id']),
            ('JSONL', f'/{model}/%s/:format/jsonl' % row['id']),
            ('ASCII', f'/{model}/%s/:format/ascii' % row['id']),
        ],
    }


def test_dataset(context, app, mocker):
    mocker.patch('spinta.backends.postgresql.dataset.get_new_id', return_value='REVISION')

    context.push([
        {
            'type': 'rinkimai/:dataset/json/:resource/data',
            'id': get_ref_id('Rinkimai 1'),
            'pavadinimas': 'Rinkimai 1',
        },
    ])

    app.authorize(['spinta_rinkimai_dataset_json_resource_data_getall'])
    resp = app.get('/rinkimai/:dataset/json/:resource/data', headers={'accept': 'text/html'})
    assert resp.status_code == 200

    resp.context.pop('request')
    assert resp.context == {
        'location': [
            ('root', '/'),
            ('rinkimai', '/rinkimai'),
            (':dataset/json/:resource/data', None),
            (':changes', '/rinkimai/:dataset/json/:resource/data/:changes'),
        ],
        'header': ['id', 'pavadinimas'],
        'data': [
            [
                {'color': None, 'link': '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:dataset/json/:resource/data', 'value': 'df6b9e04'},
                {'color': None, 'link': None, 'value': 'Rinkimai 1'},
            ],
        ],
        'row': [],
        'formats': [
            ('CSV', '/rinkimai/:dataset/json/:resource/data/:format/csv'),
            ('JSON', '/rinkimai/:dataset/json/:resource/data/:format/json'),
            ('JSONL', '/rinkimai/:dataset/json/:resource/data/:format/jsonl'),
            ('ASCII', '/rinkimai/:dataset/json/:resource/data/:format/ascii'),
        ],
    }


def test_dataset_with_show(context, app, mocker):
    mocker.patch('spinta.backends.postgresql.dataset.get_new_id', return_value='REVISION')

    context.push([
        {
            'type': 'rinkimai/:dataset/json/:resource/data',
            'id': get_ref_id('Rinkimai 1'),
            'pavadinimas': 'Rinkimai 1',
        },
    ])

    app.authorize(['spinta_rinkimai_dataset_json_resource_data_search'])
    resp = app.get('/rinkimai/:dataset/json/:show/pavadinimas', headers={'accept': 'text/html'})
    assert resp.status_code == 200

    resp.context.pop('request')
    assert resp.context['header'] == ['pavadinimas']
    assert [tuple(y['value'] for y in x) for x in resp.context['data']] == [
        ('Rinkimai 1',),
    ]


def test_dataset_url_without_resource(context, app, mocker):
    context.push([
        {
            'type': 'rinkimai/:dataset/json',
            'id': get_ref_id('Rinkimai 1'),
            'pavadinimas': 'Rinkimai 1',
        },
    ])

    app.authorize(['spinta_rinkimai_dataset_json_resource_data_getall'])
    resp = app.get('/rinkimai/:dataset/json', headers={'accept': 'text/html'})
    assert resp.status_code == 200


def test_nested_dataset(context, app, mocker):
    mocker.patch('spinta.backends.postgresql.dataset.get_new_id', return_value='REVISION')

    context.push([
        {
            'type': 'deeply/nested/model/name/:dataset/nested/dataset/name/:resource/resource',
            'id': get_ref_id('42'),
            'name': 'Nested One',
        },
    ])

    app.authorize(['spinta_deeply_nested_model_name_dataset_nest989f7f4d_getall'])
    resp = app.get('deeply/nested/model/name/:dataset/nested/dataset/name/:resource/resource', headers={'accept': 'text/html'})
    assert resp.status_code == 200

    resp.context.pop('request')
    assert resp.context == {
        'location': [
            ('root', '/'),
            ('deeply', '/deeply'),
            ('nested', '/deeply/nested'),
            ('model', '/deeply/nested/model'),
            ('name', '/deeply/nested/model/name'),
            (':dataset/nested/dataset/name/:resource/resource', None),
            (':changes', '/deeply/nested/model/name/:dataset/nested/dataset/name/:resource/resource/:changes'),
        ],
        'header': ['id', 'name'],
        'data': [
            [
                {'color': None, 'link': '/deeply/nested/model/name/e2ff1ff0f7d663344abe821582b0908925e5b366/:dataset/nested/dataset/name/:resource/resource', 'value': 'e2ff1ff0'},
                {'color': None, 'link': None, 'value': 'Nested One'},
            ],
        ],
        'row': [],
        'formats': [
            ('CSV', '/deeply/nested/model/name/:dataset/nested/dataset/name/:resource/resource/:format/csv'),
            ('JSON', '/deeply/nested/model/name/:dataset/nested/dataset/name/:resource/resource/:format/json'),
            ('JSONL', '/deeply/nested/model/name/:dataset/nested/dataset/name/:resource/resource/:format/jsonl'),
            ('ASCII', '/deeply/nested/model/name/:dataset/nested/dataset/name/:resource/resource/:format/ascii'),
        ],
    }


def test_dataset_key(context, app, mocker):
    mocker.patch('spinta.backends.postgresql.dataset.get_new_id', return_value='REVISION')

    context.push([
        {
            'type': 'rinkimai/:dataset/json/:resource/data',
            'id': get_ref_id('Rinkimai 1'),
            'pavadinimas': 'Rinkimai 1',
        },
    ])

    app.authorize(['spinta_rinkimai_dataset_json_resource_data_getone'])
    resp = app.get('/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:dataset/json/:resource/data', headers={'accept': 'text/html'})
    assert resp.status_code == 200

    resp.context.pop('request')
    assert resp.context == {
        'location': [
            ('root', '/'),
            ('rinkimai', '/rinkimai'),
            (':dataset/json/:resource/data', '/rinkimai/:dataset/json/:resource/data'),
            ('df6b9e04', None),
            (':changes', '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:dataset/json/:resource/data/:changes'),
        ],
        'formats': [
            ('CSV', '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:dataset/json/:resource/data/:format/csv'),
            ('JSON', '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:dataset/json/:resource/data/:format/json'),
            ('JSONL', '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:dataset/json/:resource/data/:format/jsonl'),
            ('ASCII', '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:dataset/json/:resource/data/:format/ascii'),
        ],
        'header': [],
        'data': [],
        'row': [
            ('id', {
                'color': None,
                'link': '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:dataset/json/:resource/data',
                'value': 'df6b9e04ac9e2467690bcad6d9fd673af6e1919b',
            }),
            ('pavadinimas', {'color': None, 'link': None, 'value': 'Rinkimai 1'}),
            ('type', {'color': None, 'link': None, 'value': 'rinkimai/:dataset/json/:resource/data'}),
            ('revision', {'color': None, 'link': None, 'value': 'REVISION'}),
        ],
    }


def test_changes_single_object(context, app, mocker):
    mocker.patch('spinta.backends.postgresql.dataset.utcnow', return_value=datetime.datetime(2019, 3, 6, 16, 15, 0, 816308))
    mocker.patch('spinta.backends.postgresql.dataset.get_new_id', return_value='REVISION')

    context.push([
        {
            'type': 'rinkimai/:dataset/json/:resource/data',
            'id': get_ref_id('Rinkimai 1'),
            'pavadinimas': 'Rinkimai 1',
        },
    ])
    context.push([
        {
            'type': 'rinkimai/:dataset/json/:resource/data',
            'id': get_ref_id('Rinkimai 1'),
            'pavadinimas': 'Rinkimai 2',
        },
    ])

    app.authorize(['spinta_rinkimai_dataset_json_resource_data_changes'])
    resp = app.get('/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:dataset/json/:resource/data/:changes', headers={'accept': 'text/html'})
    assert resp.status_code == 200

    resp.context.pop('request')
    assert resp.context == {
        'location': [
            ('root', '/'),
            ('rinkimai', '/rinkimai'),
            (':dataset/json/:resource/data', '/rinkimai/:dataset/json/:resource/data'),
            ('df6b9e04', '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:dataset/json/:resource/data'),
            (':changes', None),
        ],
        'formats': [
            ('CSV', '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:dataset/json/:resource/data/:changes/:format/csv'),
            ('JSON', '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:dataset/json/:resource/data/:changes/:format/json'),
            ('JSONL', '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:dataset/json/:resource/data/:changes/:format/jsonl'),
            ('ASCII', '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:dataset/json/:resource/data/:changes/:format/ascii'),
        ],
        'header': [
            'change_id',
            'transaction_id',
            'datetime',
            'action',
            'id',
            'pavadinimas',
        ],
        'data': [
            [
                {'color': None, 'link': None, 'value': resp.context['data'][0][0]['value']},
                {'color': None, 'link': None, 'value': resp.context['data'][0][1]['value']},
                {'color': None, 'link': None, 'value': '2019-03-06T16:15:00.816308'},
                {'color': None, 'link': None, 'value': 'patch'},
                {'color': None, 'link': '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:dataset/json/:resource/data', 'value': 'df6b9e04'},
                {'color': '#B2E2AD', 'link': None, 'value': 'Rinkimai 2'},
            ],
            [
                {'color': None, 'link': None, 'value': resp.context['data'][1][0]['value']},
                {'color': None, 'link': None, 'value': resp.context['data'][1][1]['value']},
                {'color': None, 'link': None, 'value': '2019-03-06T16:15:00.816308'},
                {'color': None, 'link': None, 'value': 'insert'},
                {'color': None, 'link': '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:dataset/json/:resource/data', 'value': 'df6b9e04'},
                {'color': '#B2E2AD', 'link': None, 'value': 'Rinkimai 1'},
            ],
        ],
        'row': [],
    }


def test_changes_object_list(context, app, mocker):
    mocker.patch('spinta.backends.postgresql.dataset.utcnow', return_value=datetime.datetime(2019, 3, 6, 16, 15, 0, 816308))
    mocker.patch('spinta.backends.postgresql.dataset.get_new_id', return_value='REVISION')

    context.push([
        {
            'type': 'rinkimai/:dataset/json/:resource/data',
            'id': get_ref_id('Rinkimai 1'),
            'pavadinimas': 'Rinkimai 1',
        },
    ])
    context.push([
        {
            'type': 'rinkimai/:dataset/json/:resource/data',
            'id': get_ref_id('Rinkimai 1'),
            'pavadinimas': 'Rinkimai 2',
        },
    ])

    app.authorize(['spinta_rinkimai_dataset_json_resource_data_changes'])
    resp = app.get('/rinkimai/:dataset/json/:resource/data/:changes', headers={'accept': 'text/html'})
    assert resp.status_code == 200

    resp.context.pop('request')
    assert resp.context == {
        'location': [
            ('root', '/'),
            ('rinkimai', '/rinkimai'),
            (':dataset/json/:resource/data', '/rinkimai/:dataset/json/:resource/data'),
            (':changes', None),
        ],
        'formats': [
            ('CSV', '/rinkimai/:dataset/json/:resource/data/:changes/:format/csv'),
            ('JSON', '/rinkimai/:dataset/json/:resource/data/:changes/:format/json'),
            ('JSONL', '/rinkimai/:dataset/json/:resource/data/:changes/:format/jsonl'),
            ('ASCII', '/rinkimai/:dataset/json/:resource/data/:changes/:format/ascii'),
        ],
        'header': [
            'change_id',
            'transaction_id',
            'datetime',
            'action',
            'id',
            'pavadinimas',
        ],
        'data': [
            [
                {'color': None, 'link': None, 'value': resp.context['data'][0][0]['value']},
                {'color': None, 'link': None, 'value': resp.context['data'][0][1]['value']},
                {'color': None, 'link': None, 'value': '2019-03-06T16:15:00.816308'},
                {'color': None, 'link': None, 'value': 'patch'},
                {'color': None, 'link': '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:dataset/json/:resource/data', 'value': 'df6b9e04'},
                {'color': '#B2E2AD', 'link': None, 'value': 'Rinkimai 2'},
            ],
            [
                {'color': None, 'link': None, 'value': resp.context['data'][1][0]['value']},
                {'color': None, 'link': None, 'value': resp.context['data'][1][1]['value']},
                {'color': None, 'link': None, 'value': '2019-03-06T16:15:00.816308'},
                {'color': None, 'link': None, 'value': 'insert'},
                {'color': None, 'link': '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:dataset/json/:resource/data', 'value': 'df6b9e04'},
                {'color': '#B2E2AD', 'link': None, 'value': 'Rinkimai 1'},
            ],
        ],
        'row': [],
    }


def test_count(context, app):
    consume(context.push([
        {
            'type': 'rinkimai/:dataset/json/:resource/data',
            'id': get_ref_id(1),
            'pavadinimas': 'Rinkimai 1',
        },
        {
            'type': 'rinkimai/:dataset/json/:resource/data',
            'id': get_ref_id(2),
            'pavadinimas': 'Rinkimai 2',
        },
    ]))

    app.authorize(['spinta_rinkimai_dataset_json_resource_data_search'])
    resp = app.get('/rinkimai/:dataset/json/:resource/data/:count', headers={'accept': 'text/html'})
    assert resp.status_code == 200

    resp.context.pop('request')
    assert resp.context['header'] == ['count']
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
    id_ = data['id']
    revision = data['revision']
    assert uuid.UUID(id_).version == 4
    assert data == {
        'id': id_,
        'type': model,
        'revision': revision,
        'report_type': None,
        'status': 'ok',
        'valid_from_date': None,
        'update_time': None,
        'count': 42,
        'notes': [],
        'operating_licenses': [],
    }

    resp = app.get(f'/{model}/{id_}')
    assert resp.status_code == 200
    assert resp.json() == {
        'id': id_,
        'type': model,
        'revision': revision,
        'report_type': None,
        'status': 'ok',
        'valid_from_date': None,
        'update_time': None,
        'count': 42,
        'notes': [],
        'operating_licenses': [],
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
        'id': '0007ddec-092b-44b5-9651-76884e6081b4',
        'status': 'ok',
        'count': 42,
    })
    assert resp.status_code == 403
    assert get_error_codes(resp.json()) == ["InsufficientScopeError"]


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


@pytest.mark.models(
    'backends/postgres/report',
    'backends/mongo/report',
)
def test_post_update_postgres(model, context, app):
    # tests if update works with `id` present in the json
    scope_model = model.replace('/', '_')
    app.authorize([
        f'spinta_{scope_model}_insert',
        'spinta_set_meta_fields',
    ])
    resp = app.post(f'/{model}', json={
        'id': '0007ddec-092b-44b5-9651-76884e6081b4',
        'status': 'ok',
        'count': 42,
    })

    assert resp.status_code == 201
    assert resp.json()['id'] == '0007ddec-092b-44b5-9651-76884e6081b4'

    # POST invalid id
    resp = app.post(f'/{model}', json={
        'id': 123,
        'status': 'not-ok',
        'count': 13,
    })

    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]

    # POST invalid id
    resp = app.post(f'/{model}', json={
        'id': '123',
        'status': 'not-ok',
        'count': 13,
    })

    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidValue"]

    # POST invalid id
    resp = app.post(f'/{model}', json={
        'id': None,
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
        'revision': 'r3v1510n',
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
    app.authmodel(model, ['insert', 'update'])
    resp = app.post(f'/{model}', json={
        'status': 'valid',
        'count': 42,
    })
    assert resp.status_code == 201
    data = resp.json()
    id_ = data['id']

    resp = app.post(f'/{model}', json={
        'id': id_,
        'status': 'valid',
        'count': 42,
    })
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["UniqueConstraint"]


@pytest.mark.skip("SPLAT-146: PATCH requests should be able to update id")
@pytest.mark.models(
    'backends/postgres/report',
    'backends/mongo/report',
)
def test_patch_duplicate_id(model, context, app):
    # tests that duplicate ID detection works with PATCH requests
    app.authorize(
        get_model_scopes(context, model, ['insert', 'getone', 'patch']) +
        ['spinta_set_meta_fields'],
    )

    report_data = app.post(f'/{model}', json={
        'type': 'report',
        'status': '1',
    }).json()
    id_ = report_data['id']

    # try to PATCH id with set_meta_fields scope
    # this should be successful because id that we want to setup
    # does not exist in database
    resp = app.patch(f'/{model}/{id_}', json={
        'id': '0007ddec-092b-44b5-9651-76884e6081b4',
    })
    assert resp.status_code == 200
    id_ = resp.json()['id']
    assert id_ == '0007ddec-092b-44b5-9651-76884e6081b4'

    # try to patch report with id of itself
    resp = app.patch(f'/{model}/{id_}', json={
        'id': '0007ddec-092b-44b5-9651-76884e6081b4',
    })
    assert resp.status_code == 200
    assert resp.json()['id'] == '0007ddec-092b-44b5-9651-76884e6081b4'

    # try to PATCH id with set_meta_fields scope
    # this should not be successful because id that we want to setup
    # already exists in database
    resp = app.post(f'/{model}', json={
        'type': 'report',
        'status': '1',
    })
    existing_id = resp.json()['id']

    resp = app.patch(f'/{model}/{id_}',
                     json={'id': existing_id})
    assert resp.status_code == 400
    assert resp.json() == {"error": f"'id' is unique for '{model}' and a duplicate value is found in database."}


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
def test_streaming_response(model, context, app):
    consume(context.push([
        {
            'type': model,
            'count': 42,
            'status': 'ok',
        },
        {
            'type': model,
            'count': 13,
            'status': 'not-ok',
        },
    ]))

    app.authmodel(model, ['getall'])
    resp = app.get(f'/{model}').json()
    data = resp['data']
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
    app.authmodel(model, ['insert'])
    resp = app.post(f'/{model}', json={'status': '42'})
    assert resp.status_code == 201
