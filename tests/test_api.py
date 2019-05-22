import datetime

import pytest

from spinta.utils.itertools import consume


def test_app(app):
    resp = app.get('/', headers={'accept': 'text/html'})
    assert resp.status_code == 200

    resp.context.pop('request')
    assert resp.context == {
        'location': [
            ('root', '/'),
        ],
        'items': [
            ('capital', '/capital'),
            ('continent', '/continent'),
            ('country', '/country'),
            ('deeply', '/deeply'),
            ('nested', '/nested'),
            ('org', '/org'),
            ('report', '/report'),
            ('rinkimai', '/rinkimai'),
            ('tenure', '/tenure'),
        ],
        'datasets': [],
        'header': [],
        'data': [],
        'row': [],
        'formats': [],
    }


def test_directory(app):
    resp = app.get('/rinkimai', headers={'accept': 'text/html'})
    assert resp.status_code == 200

    resp.context.pop('request')
    assert resp.context == {
        'location': [
            ('root', '/'),
            ('rinkimai', None),
        ],
        'items': [
            ('apygarda', '/rinkimai/apygarda'),
            ('apylinke', '/rinkimai/apylinke'),
            ('kandidatas', '/rinkimai/kandidatas'),
            ('turas', '/rinkimai/turas'),
        ],
        'datasets': [
            {'name': 'json', 'link': '/rinkimai/:source/json', 'canonical': False},
            {'name': 'xlsx', 'link': '/rinkimai/:source/xlsx', 'canonical': False},
        ],
        'header': [],
        'data': [],
        'row': [],
        'formats': [],
    }


def test_model(context, app):
    row, = context.push([
        {
            'type': 'country',
            'title': 'Earth',
            'code': 'er',
        },
    ])

    app.authorize(['spinta_country_getall'])
    resp = app.get('/country', headers={'accept': 'text/html'})
    assert resp.status_code == 200

    resp.context.pop('request')
    assert resp.context == {
        'location': [
            ('root', '/'),
            ('country', None),
            (':changes', '/country/:changes'),
        ],
        'items': [],
        'datasets': [],
        'header': ['id', 'title', 'code'],
        'data': [
            [
                {'color': None, 'link': '/country/%s' % row['id'], 'value': row['id']},
                {'color': None, 'link': None, 'value': 'Earth'},
                {'color': None, 'link': None, 'value': 'er'},
            ],
        ],
        'row': [],
        'formats': [
            ('CSV', '/country/:format/csv'),
            ('JSON', '/country/:format/json'),
            ('JSONL', '/country/:format/jsonl'),
            ('ASCII', '/country/:format/ascii'),
        ],
    }


def test_model_get(context, app):
    row, = context.push([
        {
            'type': 'country',
            'title': 'Earth',
            'code': 'er',
        },
    ])

    app.authorize(['spinta_country_getone'])
    resp = app.get('/country/%s' % row['id'], headers={'accept': 'text/html'})
    assert resp.status_code == 200

    resp.context.pop('request')
    assert resp.context == {
        'location': [
            ('root', '/'),
            ('country', '/country'),
            (row['id'], None),
            (':changes', '/country/%s/:changes' % row['id']),
        ],
        'items': [],
        'datasets': [],
        'header': [],
        'data': [],
        'row': [
            ('type', {'color': None, 'link': None, 'value': 'country'}),
            ('id', {'color': None, 'link': '/country/%s' % row['id'], 'value': row['id']}),
            ('title', {'color': None, 'link': None, 'value': 'Earth'}),
            ('code', {'color': None, 'link': None, 'value': 'er'}),
        ],
        'formats': [
            ('CSV', '/country/%s/:format/csv' % row['id']),
            ('JSON', '/country/%s/:format/json' % row['id']),
            ('JSONL', '/country/%s/:format/jsonl' % row['id']),
            ('ASCII', '/country/%s/:format/ascii' % row['id']),
        ],
    }


def test_dataset(context, app):
    consume(context.push([
        {
            'type': 'rinkimai/:source/json',
            'id': 'Rinkimai 1',
            'pavadinimas': 'Rinkimai 1',
        },
    ]))

    app.authorize(['spinta_rinkimai_source_json_getall'])
    resp = app.get('/rinkimai/:source/json', headers={'accept': 'text/html'})
    assert resp.status_code == 200

    resp.context.pop('request')
    assert resp.context == {
        'location': [
            ('root', '/'),
            ('rinkimai', '/rinkimai'),
            (':source/json', None),
            (':changes', '/rinkimai/:source/json/:changes'),
        ],
        'items': [],
        'datasets': [],
        'header': ['id', 'pavadinimas'],
        'data': [
            [
                {'color': None, 'link': '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:source/json', 'value': 'df6b9e04'},
                {'color': None, 'link': None, 'value': 'Rinkimai 1'},
            ],
        ],
        'row': [],
        'formats': [
            ('CSV', '/rinkimai/:source/json/:format/csv'),
            ('JSON', '/rinkimai/:source/json/:format/json'),
            ('JSONL', '/rinkimai/:source/json/:format/jsonl'),
            ('ASCII', '/rinkimai/:source/json/:format/ascii'),
        ],
    }


def test_nested_dataset(context, app):
    consume(context.push([
        {
            'type': 'deeply/nested/model/name/:source/nested/dataset/name',
            'id': '42',
            'name': 'Nested One',
        },
    ]))

    app.authorize(['spinta_deeply_nested_model_name_source_neste168fff41_getall'])
    resp = app.get('deeply/nested/model/name/:source/nested/dataset/name', headers={'accept': 'text/html'})
    assert resp.status_code == 200

    resp.context.pop('request')
    assert resp.context == {
        'location': [
            ('root', '/'),
            ('deeply', '/deeply'),
            ('nested', '/deeply/nested'),
            ('model', '/deeply/nested/model'),
            ('name', '/deeply/nested/model/name'),
            (':source/nested/dataset/name', None),
            (':changes', '/deeply/nested/model/name/:source/nested/dataset/name/:changes'),
        ],
        'items': [],
        'datasets': [],
        'header': ['id', 'name'],
        'data': [
            [
                {'color': None, 'link': '/deeply/nested/model/name/e2ff1ff0f7d663344abe821582b0908925e5b366/:source/nested/dataset/name', 'value': 'e2ff1ff0'},
                {'color': None, 'link': None, 'value': 'Nested One'},
            ],
        ],
        'row': [],
        'formats': [
            ('CSV', '/deeply/nested/model/name/:source/nested/dataset/name/:format/csv'),
            ('JSON', '/deeply/nested/model/name/:source/nested/dataset/name/:format/json'),
            ('JSONL', '/deeply/nested/model/name/:source/nested/dataset/name/:format/jsonl'),
            ('ASCII', '/deeply/nested/model/name/:source/nested/dataset/name/:format/ascii'),
        ],
    }


def test_dataset_key(context, app):
    consume(context.push([
        {
            'type': 'rinkimai/:source/json',
            'id': 'Rinkimai 1',
            'pavadinimas': 'Rinkimai 1',
        },
    ]))

    app.authorize(['spinta_rinkimai_source_json_getone'])
    resp = app.get('/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:source/json', headers={'accept': 'text/html'})
    assert resp.status_code == 200

    resp.context.pop('request')
    assert resp.context == {
        'location': [
            ('root', '/'),
            ('rinkimai', '/rinkimai'),
            (':source/json', '/rinkimai/:source/json'),
            ('df6b9e04', None),
            (':changes', '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:source/json/:changes'),
        ],
        'formats': [
            ('CSV', '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:source/json/:format/csv'),
            ('JSON', '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:source/json/:format/json'),
            ('JSONL', '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:source/json/:format/jsonl'),
            ('ASCII', '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:source/json/:format/ascii'),
        ],
        'datasets': [],
        'items': [],
        'header': [],
        'data': [],
        'row': [
            ('id', {
                'color': None,
                'link': '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:source/json',
                'value': 'df6b9e04ac9e2467690bcad6d9fd673af6e1919b',
            }),
            ('pavadinimas', {'color': None, 'link': None, 'value': 'Rinkimai 1'}),
            ('type', {'color': None, 'link': None, 'value': 'rinkimai/:source/json'}),
        ],
    }


def test_changes_single_object(context, app, mocker):
    mocker.patch('spinta.backends.postgresql.dataset.utcnow', return_value=datetime.datetime(2019, 3, 6, 16, 15, 0, 816308))

    consume(context.push([
        {
            'type': 'rinkimai/:source/json',
            'id': 'Rinkimai 1',
            'pavadinimas': 'Rinkimai 1',
        },
    ]))
    consume(context.push([
        {
            'type': 'rinkimai/:source/json',
            'id': 'Rinkimai 1',
            'pavadinimas': 'Rinkimai 2',
        },
    ]))

    app.authorize(['spinta_rinkimai_source_json_changes'])
    resp = app.get('/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:source/json/:changes', headers={'accept': 'text/html'})
    assert resp.status_code == 200

    resp.context.pop('request')
    assert resp.context == {
        'location': [
            ('root', '/'),
            ('rinkimai', '/rinkimai'),
            (':source/json', '/rinkimai/:source/json'),
            ('df6b9e04', '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:source/json'),
            (':changes', None),
        ],
        'formats': [
            ('CSV', '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:source/json/:changes/:format/csv'),
            ('JSON', '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:source/json/:changes/:format/json'),
            ('JSONL', '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:source/json/:changes/:format/jsonl'),
            ('ASCII', '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:source/json/:changes/:format/ascii'),
        ],
        'datasets': [],
        'items': [],
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
                {'color': None, 'link': None, 'value': 'update'},
                {'color': None, 'link': '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:source/json', 'value': 'df6b9e04'},
                {'color': '#B2E2AD', 'link': None, 'value': 'Rinkimai 2'},
            ],
            [
                {'color': None, 'link': None, 'value': resp.context['data'][1][0]['value']},
                {'color': None, 'link': None, 'value': resp.context['data'][1][1]['value']},
                {'color': None, 'link': None, 'value': '2019-03-06T16:15:00.816308'},
                {'color': None, 'link': None, 'value': 'insert'},
                {'color': None, 'link': '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:source/json', 'value': 'df6b9e04'},
                {'color': '#B2E2AD', 'link': None, 'value': 'Rinkimai 1'},
            ],
        ],
        'row': [],
    }


def test_changes_object_list(context, app, mocker):
    mocker.patch('spinta.backends.postgresql.dataset.utcnow', return_value=datetime.datetime(2019, 3, 6, 16, 15, 0, 816308))

    consume(context.push([
        {
            'type': 'rinkimai/:source/json',
            'id': 'Rinkimai 1',
            'pavadinimas': 'Rinkimai 1',
        },
    ]))
    consume(context.push([
        {
            'type': 'rinkimai/:source/json',
            'id': 'Rinkimai 1',
            'pavadinimas': 'Rinkimai 2',
        },
    ]))

    app.authorize(['spinta_rinkimai_source_json_changes'])
    resp = app.get('/rinkimai/:source/json/:changes', headers={'accept': 'text/html'})
    assert resp.status_code == 200

    resp.context.pop('request')
    assert resp.context == {
        'location': [
            ('root', '/'),
            ('rinkimai', '/rinkimai'),
            (':source/json', '/rinkimai/:source/json'),
            (':changes', None),
        ],
        'formats': [
            ('CSV', '/rinkimai/:source/json/:changes/:format/csv'),
            ('JSON', '/rinkimai/:source/json/:changes/:format/json'),
            ('JSONL', '/rinkimai/:source/json/:changes/:format/jsonl'),
            ('ASCII', '/rinkimai/:source/json/:changes/:format/ascii'),
        ],
        'datasets': [],
        'items': [],
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
                {'color': None, 'link': None, 'value': 'update'},
                {'color': None, 'link': '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:source/json', 'value': 'df6b9e04'},
                {'color': '#B2E2AD', 'link': None, 'value': 'Rinkimai 2'},
            ],
            [
                {'color': None, 'link': None, 'value': resp.context['data'][1][0]['value']},
                {'color': None, 'link': None, 'value': resp.context['data'][1][1]['value']},
                {'color': None, 'link': None, 'value': '2019-03-06T16:15:00.816308'},
                {'color': None, 'link': None, 'value': 'insert'},
                {'color': None, 'link': '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:source/json', 'value': 'df6b9e04'},
                {'color': '#B2E2AD', 'link': None, 'value': 'Rinkimai 1'},
            ],
        ],
        'row': [],
    }


def test_count(context, app):
    consume(context.push([
        {
            'type': 'rinkimai/:source/json',
            'id': 1,
            'pavadinimas': 'Rinkimai 1',
        },
        {
            'type': 'rinkimai/:source/json',
            'id': 2,
            'pavadinimas': 'Rinkimai 2',
        },
    ]))

    app.authorize(['spinta_rinkimai_source_json_getall'])
    resp = app.get('/rinkimai/:source/json/:count', headers={'accept': 'text/html'})
    assert resp.status_code == 200

    resp.context.pop('request')
    assert resp.context['header'] == ['count']
    assert resp.context['data'] == [[{'color': None, 'link': None, 'value': 2}]]


def test_post(context, app):
    app.authorize([
        'spinta_country_insert',
        'spinta_country_getone',
    ])

    # tests basic object creation
    resp = app.post('/country', json={
        'title': 'Earth',
        'code': 'er',
    })
    assert resp.status_code == 201
    data = resp.json()
    assert data == {'id': data['id'], 'type': 'country'}

    id = data['id']
    resp = app.get(f'/country/{id}')
    assert resp.status_code == 200
    assert resp.json() == {
        'type': 'country',
        'id': id,
        'code': 'er',
        'title': 'Earth',
    }


def test_post_invalid_json(context, app):
    # tests 400 response on invalid json
    app.authorize(['spinta_country_getall'])
    headers = {"content-type": "application/json"}
    resp = app.post('/country', headers=headers, data="""{
        "title": "Earth",
        "code": "er"
    ]""")
    assert resp.status_code == 400
    assert resp.json() == {"error": "not a valid json"}


def test_post_empty_content(context, app):
    # tests posting empty content
    app.authorize(['spinta_country_getall'])
    headers = {
        "content-length": "0",
        "content-type": "application/json",
    }
    resp = app.post('/country', headers=headers, json=None)
    assert resp.status_code == 400
    assert resp.json() == {"error": "not a valid json"}


def test_post_id(context, app):
    # tests 400 response when trying to create object with id
    app.authorize(['spinta_country_insert'])
    resp = app.post('/country', json={
        'id': '42',
        'title': 'Earth',
        'code': 'er',
    })
    assert resp.status_code == 403
    assert resp.json() == {"error": "insufficient_scope"}


def test_post_update(context, app):
    # tests if update works with `id` present in the json
    app.authorize([
        'spinta_country_insert',
        'spinta_set_meta_fields',
    ])
    resp = app.post('/country', json={
        'id': '42',
        'title': 'Earth',
        'code': 'er',
    })

    assert resp.status_code == 201
    assert resp.json()['id'] == '42'


def test_post_revision(context, app):
    # tests 400 response when trying to create object with revision
    app.authorize(['spinta_country_insert'])
    resp = app.post('/country', json={
        'revision': 'r3v1510n',
        'title': 'Earth',
        'code': 'er',
    })
    assert resp.status_code == 400
    assert resp.json() == {"error": "cannot create 'revision'"}


@pytest.mark.skip('TODO')
def test_post_duplicate_id(context, app):
    # tests 400 response when trying to create object with id which exists
    app.authorize([
        'spinta_country_insert',
        'spinta_country_update',
        'spinta_set_meta_fields',
    ])
    resp = app.post('/country', json={
        'title': 'Earth',
        'code': 'er',
    })
    assert resp.status_code == 201
    data = resp.json()
    id = data['id']

    # TODO: this raises:
    #
    #           sqlalchemy.exc.IntegrityError: (psycopg2.errors.UniqueViolation) duplicate key value violates unique constraint
    #
    #       Should be handled some how and returned 400 error.
    resp = app.post('/country', json={
        'id': id,
        'title': 'Earth',
        'code': 'er',
    })
    assert resp.status_code == 400
    assert resp.json() == {"error": "cannot create duplicate 'id'"}


def test_post_non_json_content_type(context, app):
    # tests 400 response when trying to make non-json request
    app.authorize(['spinta_country_insert'])
    headers = {"content-type": "application/text"}
    resp = app.post('/country', headers=headers, json={
        "title": "Earth",
        "code": "er"
    })
    assert resp.status_code == 415
    assert resp.json() == {"error": "only 'application/json' content-type is supported"}


def test_post_bad_auth_header(context, app):
    # tests 400 response when authorization header is missing `Bearer `
    auth_header = {'authorization': 'Fail f00b4rb4z'}
    resp = app.post('/country', headers=auth_header, json={
        'title': 'Earth',
        'code': 'er',
    })
    assert resp.status_code == 401
    assert resp.json() == {'error': 'unsupported_token_type'}


def test_streaming_response(context, app):
    consume(context.push([
        {
            'type': 'country',
            'code': 'fi',
            'title': 'Finland',
        },
        {
            'type': 'country',
            'code': 'lt',
            'title': 'Lithuania',
        },
    ]))

    app.authorize(['spinta_country_getall'])
    resp = app.get('/country').json()
    data = resp['data']
    data = sorted((x['code'], x['title']) for x in data)
    assert data == [
        ('fi', 'Finland'),
        ('lt', 'Lithuania'),
    ]
