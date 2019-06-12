import uuid
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
            ('photo', '/photo'),
            # FIXME: /report should be /reports, because that is specified in
            #        'endpoint' option of report model.
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
            {'name': 'json/data', 'link': '/rinkimai/:ds/json/:rs/data', 'canonical': False},
            {'name': 'xlsx/data', 'link': '/rinkimai/:ds/xlsx/:rs/data', 'canonical': False},
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
        'datasets': [
            {'canonical': False, 'link': '/country/:ds/csv/:rs/countries', 'name': 'csv/countries'},
            {'canonical': False, 'link': '/country/:ds/denorm/:rs/orgs', 'name': 'denorm/orgs'},
            {'canonical': False, 'link': '/country/:ds/dependencies/:rs/continents', 'name': 'dependencies/continents'},
            {'canonical': False, 'link': '/country/:ds/sql/:rs/db', 'name': 'sql/db'},
        ],
        'header': ['id', 'title', 'code'],
        'data': [
            [
                {'color': None, 'link': '/country/%s' % row['id'], 'value': row['id'][:8]},
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
            (row['id'][:8], None),
            (':changes', '/country/%s/:changes' % row['id']),
        ],
        'items': [],
        'datasets': [
            {'canonical': False, 'link': '/country/:ds/csv/:rs/countries', 'name': 'csv/countries'},
            {'canonical': False, 'link': '/country/:ds/denorm/:rs/orgs', 'name': 'denorm/orgs'},
            {'canonical': False, 'link': '/country/:ds/dependencies/:rs/continents', 'name': 'dependencies/continents'},
            {'canonical': False, 'link': '/country/:ds/sql/:rs/db', 'name': 'sql/db'},
        ],
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
            'type': 'rinkimai/:ds/json/:rs/data',
            'id': 'Rinkimai 1',
            'pavadinimas': 'Rinkimai 1',
        },
    ]))

    app.authorize(['spinta_rinkimai_ds_json_rs_data_getall'])
    resp = app.get('/rinkimai/:ds/json/:rs/data', headers={'accept': 'text/html'})
    assert resp.status_code == 200

    resp.context.pop('request')
    assert resp.context == {
        'location': [
            ('root', '/'),
            ('rinkimai', '/rinkimai'),
            (':ds/json/:rs/data', None),
            (':changes', '/rinkimai/:ds/json/:rs/data/:changes'),
        ],
        'items': [
            ('apygarda', '/rinkimai/apygarda'),
            ('apylinke', '/rinkimai/apylinke'),
            ('kandidatas', '/rinkimai/kandidatas'),
            ('turas', '/rinkimai/turas'),
        ],
        'datasets': [
            {'canonical': False, 'link': '/rinkimai/:ds/json/:rs/data', 'name': 'json/data'},
            {'canonical': False, 'link': '/rinkimai/:ds/xlsx/:rs/data', 'name': 'xlsx/data'},
        ],
        'header': ['id', 'pavadinimas'],
        'data': [
            [
                {'color': None, 'link': '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:ds/json/:rs/data', 'value': 'df6b9e04'},
                {'color': None, 'link': None, 'value': 'Rinkimai 1'},
            ],
        ],
        'row': [],
        'formats': [
            ('CSV', '/rinkimai/:ds/json/:rs/data/:format/csv'),
            ('JSON', '/rinkimai/:ds/json/:rs/data/:format/json'),
            ('JSONL', '/rinkimai/:ds/json/:rs/data/:format/jsonl'),
            ('ASCII', '/rinkimai/:ds/json/:rs/data/:format/ascii'),
        ],
    }


def test_nested_dataset(context, app):
    consume(context.push([
        {
            'type': 'deeply/nested/model/name/:ds/nested/dataset/name/:rs/resource',
            'id': '42',
            'name': 'Nested One',
        },
    ]))

    app.authorize(['spinta_deeply_nested_model_name_ds_nested_da9add3385_getall'])
    resp = app.get('deeply/nested/model/name/:ds/nested/dataset/name/:rs/resource', headers={'accept': 'text/html'})
    assert resp.status_code == 200

    resp.context.pop('request')
    assert resp.context == {
        'location': [
            ('root', '/'),
            ('deeply', '/deeply'),
            ('nested', '/deeply/nested'),
            ('model', '/deeply/nested/model'),
            ('name', '/deeply/nested/model/name'),
            (':ds/nested/dataset/name/:rs/resource', None),
            (':changes', '/deeply/nested/model/name/:ds/nested/dataset/name/:rs/resource/:changes'),
        ],
        'items': [],
        'datasets': [
            {
                'canonical': False,
                'link': '/deeply/nested/model/name/:ds/nested/dataset/name/:rs/resource',
                'name': 'nested/dataset/name/resource',
            },
        ],
        'header': ['id', 'name'],
        'data': [
            [
                {'color': None, 'link': '/deeply/nested/model/name/e2ff1ff0f7d663344abe821582b0908925e5b366/:ds/nested/dataset/name/:rs/resource', 'value': 'e2ff1ff0'},
                {'color': None, 'link': None, 'value': 'Nested One'},
            ],
        ],
        'row': [],
        'formats': [
            ('CSV', '/deeply/nested/model/name/:ds/nested/dataset/name/:rs/resource/:format/csv'),
            ('JSON', '/deeply/nested/model/name/:ds/nested/dataset/name/:rs/resource/:format/json'),
            ('JSONL', '/deeply/nested/model/name/:ds/nested/dataset/name/:rs/resource/:format/jsonl'),
            ('ASCII', '/deeply/nested/model/name/:ds/nested/dataset/name/:rs/resource/:format/ascii'),
        ],
    }


def test_dataset_key(context, app):
    consume(context.push([
        {
            'type': 'rinkimai/:ds/json/:rs/data',
            'id': 'Rinkimai 1',
            'pavadinimas': 'Rinkimai 1',
        },
    ]))

    app.authorize(['spinta_rinkimai_ds_json_rs_data_getone'])
    resp = app.get('/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:ds/json/:rs/data', headers={'accept': 'text/html'})
    assert resp.status_code == 200

    resp.context.pop('request')
    assert resp.context == {
        'location': [
            ('root', '/'),
            ('rinkimai', '/rinkimai'),
            (':ds/json/:rs/data', '/rinkimai/:ds/json/:rs/data'),
            ('df6b9e04', None),
            (':changes', '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:ds/json/:rs/data/:changes'),
        ],
        'formats': [
            ('CSV', '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:ds/json/:rs/data/:format/csv'),
            ('JSON', '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:ds/json/:rs/data/:format/json'),
            ('JSONL', '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:ds/json/:rs/data/:format/jsonl'),
            ('ASCII', '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:ds/json/:rs/data/:format/ascii'),
        ],
        'datasets': [
            {'canonical': False, 'link': '/rinkimai/:ds/json/:rs/data', 'name': 'json/data'},
            {'canonical': False, 'link': '/rinkimai/:ds/xlsx/:rs/data', 'name': 'xlsx/data'},
        ],
        'items': [
            ('apygarda', '/rinkimai/apygarda'),
            ('apylinke', '/rinkimai/apylinke'),
            ('kandidatas', '/rinkimai/kandidatas'),
            ('turas', '/rinkimai/turas'),
        ],
        'header': [],
        'data': [],
        'row': [
            ('id', {
                'color': None,
                'link': '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:ds/json/:rs/data',
                'value': 'df6b9e04ac9e2467690bcad6d9fd673af6e1919b',
            }),
            ('pavadinimas', {'color': None, 'link': None, 'value': 'Rinkimai 1'}),
            ('type', {'color': None, 'link': None, 'value': 'rinkimai/:ds/json/:rs/data'}),
        ],
    }


def test_changes_single_object(context, app, mocker):
    mocker.patch('spinta.backends.postgresql.dataset.utcnow', return_value=datetime.datetime(2019, 3, 6, 16, 15, 0, 816308))

    consume(context.push([
        {
            'type': 'rinkimai/:ds/json/:rs/data',
            'id': 'Rinkimai 1',
            'pavadinimas': 'Rinkimai 1',
        },
    ]))
    consume(context.push([
        {
            'type': 'rinkimai/:ds/json/:rs/data',
            'id': 'Rinkimai 1',
            'pavadinimas': 'Rinkimai 2',
        },
    ]))

    app.authorize(['spinta_rinkimai_ds_json_rs_data_changes'])
    resp = app.get('/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:ds/json/:rs/data/:changes', headers={'accept': 'text/html'})
    assert resp.status_code == 200

    resp.context.pop('request')
    assert resp.context == {
        'location': [
            ('root', '/'),
            ('rinkimai', '/rinkimai'),
            (':ds/json/:rs/data', '/rinkimai/:ds/json/:rs/data'),
            ('df6b9e04', '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:ds/json/:rs/data'),
            (':changes', None),
        ],
        'formats': [
            ('CSV', '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:ds/json/:rs/data/:changes/:format/csv'),
            ('JSON', '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:ds/json/:rs/data/:changes/:format/json'),
            ('JSONL', '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:ds/json/:rs/data/:changes/:format/jsonl'),
            ('ASCII', '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:ds/json/:rs/data/:changes/:format/ascii'),
        ],
        'datasets': [
            {'canonical': False, 'link': '/rinkimai/:ds/json/:rs/data', 'name': 'json/data'},
            {'canonical': False, 'link': '/rinkimai/:ds/xlsx/:rs/data', 'name': 'xlsx/data'},
        ],
        'items': [
            ('apygarda', '/rinkimai/apygarda'),
            ('apylinke', '/rinkimai/apylinke'),
            ('kandidatas', '/rinkimai/kandidatas'),
            ('turas', '/rinkimai/turas'),
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
                {'color': None, 'link': None, 'value': 'update'},
                {'color': None, 'link': '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:ds/json/:rs/data', 'value': 'df6b9e04'},
                {'color': '#B2E2AD', 'link': None, 'value': 'Rinkimai 2'},
            ],
            [
                {'color': None, 'link': None, 'value': resp.context['data'][1][0]['value']},
                {'color': None, 'link': None, 'value': resp.context['data'][1][1]['value']},
                {'color': None, 'link': None, 'value': '2019-03-06T16:15:00.816308'},
                {'color': None, 'link': None, 'value': 'insert'},
                {'color': None, 'link': '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:ds/json/:rs/data', 'value': 'df6b9e04'},
                {'color': '#B2E2AD', 'link': None, 'value': 'Rinkimai 1'},
            ],
        ],
        'row': [],
    }


def test_changes_object_list(context, app, mocker):
    mocker.patch('spinta.backends.postgresql.dataset.utcnow', return_value=datetime.datetime(2019, 3, 6, 16, 15, 0, 816308))

    consume(context.push([
        {
            'type': 'rinkimai/:ds/json/:rs/data',
            'id': 'Rinkimai 1',
            'pavadinimas': 'Rinkimai 1',
        },
    ]))
    consume(context.push([
        {
            'type': 'rinkimai/:ds/json/:rs/data',
            'id': 'Rinkimai 1',
            'pavadinimas': 'Rinkimai 2',
        },
    ]))

    app.authorize(['spinta_rinkimai_ds_json_rs_data_changes'])
    resp = app.get('/rinkimai/:ds/json/:rs/data/:changes', headers={'accept': 'text/html'})
    assert resp.status_code == 200

    resp.context.pop('request')
    assert resp.context == {
        'location': [
            ('root', '/'),
            ('rinkimai', '/rinkimai'),
            (':ds/json/:rs/data', '/rinkimai/:ds/json/:rs/data'),
            (':changes', None),
        ],
        'formats': [
            ('CSV', '/rinkimai/:ds/json/:rs/data/:changes/:format/csv'),
            ('JSON', '/rinkimai/:ds/json/:rs/data/:changes/:format/json'),
            ('JSONL', '/rinkimai/:ds/json/:rs/data/:changes/:format/jsonl'),
            ('ASCII', '/rinkimai/:ds/json/:rs/data/:changes/:format/ascii'),
        ],
        'datasets': [
            {'canonical': False, 'link': '/rinkimai/:ds/json/:rs/data', 'name': 'json/data'},
            {'canonical': False, 'link': '/rinkimai/:ds/xlsx/:rs/data', 'name': 'xlsx/data'},
        ],
        'items': [
            ('apygarda', '/rinkimai/apygarda'),
            ('apylinke', '/rinkimai/apylinke'),
            ('kandidatas', '/rinkimai/kandidatas'),
            ('turas', '/rinkimai/turas'),
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
                {'color': None, 'link': None, 'value': 'update'},
                {'color': None, 'link': '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:ds/json/:rs/data', 'value': 'df6b9e04'},
                {'color': '#B2E2AD', 'link': None, 'value': 'Rinkimai 2'},
            ],
            [
                {'color': None, 'link': None, 'value': resp.context['data'][1][0]['value']},
                {'color': None, 'link': None, 'value': resp.context['data'][1][1]['value']},
                {'color': None, 'link': None, 'value': '2019-03-06T16:15:00.816308'},
                {'color': None, 'link': None, 'value': 'insert'},
                {'color': None, 'link': '/rinkimai/df6b9e04ac9e2467690bcad6d9fd673af6e1919b/:ds/json/:rs/data', 'value': 'df6b9e04'},
                {'color': '#B2E2AD', 'link': None, 'value': 'Rinkimai 1'},
            ],
        ],
        'row': [],
    }


def test_count(context, app):
    consume(context.push([
        {
            'type': 'rinkimai/:ds/json/:rs/data',
            'id': 1,
            'pavadinimas': 'Rinkimai 1',
        },
        {
            'type': 'rinkimai/:ds/json/:rs/data',
            'id': 2,
            'pavadinimas': 'Rinkimai 2',
        },
    ]))

    app.authorize(['spinta_rinkimai_ds_json_rs_data_search'])
    resp = app.get('/rinkimai/:ds/json/:rs/data/:count', headers={'accept': 'text/html'})
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
    id_ = data['id']
    assert uuid.UUID(id_).version == 4
    assert data == {
        'id': id_,
        'type': 'country',
        'code': 'er',
        'title': 'Earth',
    }

    resp = app.get(f'/country/{id_}')
    assert resp.status_code == 200
    assert resp.json() == {
        'type': 'country',
        'id': id_,
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
        'id': '0007ddec-092b-44b5-9651-76884e6081b4',
        'title': 'Earth',
        'code': 'er',
    })

    assert resp.status_code == 201
    assert resp.json()['id'] == '0007ddec-092b-44b5-9651-76884e6081b4'


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
