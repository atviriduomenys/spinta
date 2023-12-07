import base64

from _pytest.fixtures import FixtureRequest

from spinta.core.config import RawConfig
from spinta.testing.client import TestClient
from spinta.testing.client import create_test_client
from spinta.testing.manifest import bootstrap_manifest
from spinta.testing.csv import parse_csv
from spinta.testing.data import pushdata


def test_export_csv(app):
    app.authorize(['spinta_set_meta_fields'])
    app.authmodel('datasets/csv/country', [
        'insert',
        'patch',
        'getall',
        'search',
        'changes',
    ])

    resp = app.post('/datasets/csv/country', json={'_data': [
        {
            '_op': 'insert',
            '_type': 'datasets/csv/country',
            'code': 'lt',
            'title': 'Lithuania',
        },
        {
            '_op': 'insert',
            '_type': 'datasets/csv/country',
            'code': 'lv',
            'title': 'LATVIA',
        },
    ]})
    assert resp.status_code == 200, resp.json()
    data = resp.json()['_data']
    lv = data[1]
    resp = app.patch(f'/datasets/csv/country/{lv["_id"]}/', json={
        '_revision': lv['_revision'],
        'title': 'Latvia',
    })
    assert resp.status_code == 200, resp.json()

    assert app.get(
        '/datasets/csv/country/:format/csv?select(code,title)&sort(+code)'
    ).text == (
        'code,title\r\n'
        'lt,Lithuania\r\n'
        'lv,Latvia\r\n'
    )

    resp = app.get('/datasets/csv/country/:changes/:format/csv')
    assert resp.status_code == 200
    assert resp.headers['content-disposition'] == 'attachment; filename="country.csv"'
    header, *lines = resp.text.splitlines()
    header = header.split(',')
    assert header == [
        '_cid',
        '_created',
        '_op',
        '_id',
        '_txn',
        '_revision',
        'code',
        'title',
    ]
    lines = (dict(zip(header, line.split(','))) for line in lines)
    lines = [
        (
            x['_op'],
            x['code'],
            x['title'],
        )
        for x in lines
    ]
    assert lines == [
        ('insert', 'lt', 'Lithuania'),
        ('insert', 'lv', 'LATVIA'),
        ('patch', '', 'Latvia'),
    ]


def test_csv_limit(app: TestClient):
    app.authmodel('country', ['insert', 'search', ])
    resp = app.post('/country', json={'_data': [
        {'_op': 'insert', '_type': 'country', 'code': 'lt', 'title': 'Lithuania'},
        {'_op': 'insert', '_type': 'country', 'code': 'lv', 'title': 'Latvia'},
        {'_op': 'insert', '_type': 'country', 'code': 'ee', 'title': 'Estonia'},
    ]})
    assert resp.status_code == 200, resp.json()

    resp = app.get('/country/:format/csv?select(code,title)&sort(code)&limit(1)')
    assert parse_csv(resp) == [
        ['code', 'title'],
        ['ee', 'Estonia'],
    ]


def test_csv_ref_dtype(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property | type   | ref     | access
    example/csv/ref          |        |         |
      |   |   | Country      |        | name    |
      |   |   |   | name     | string |         | open
      |   |   | City         |        | name    |
      |   |   |   | name     | string |         | open
      |   |   |   | country  | ref    | Country | open
    ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authmodel('example/csv/ref', ['insert', 'search'])

    # Add data
    country = pushdata(app, '/example/csv/ref/Country', {'name': 'Lithuania'})
    pushdata(app, '/example/csv/ref/City', {
        'name': 'Vilnius',
        'country': {'_id': country['_id']},
    })

    assert parse_csv(app.get('/example/csv/ref/City/:format/csv?select(name, country)')) == [
        ['name', 'country._id'],
        ['Vilnius', country['_id']],
    ]


def test_csv_file_dtype(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property | type   | ref  | access
    example/csv/file         |        |      |
      |   |   | Country      |        | name |
      |   |   |   | name     | string |      | open
      |   |   |   | flag     | file   |      | open
    ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authmodel('example/csv/file', ['insert', 'search'])

    # Add data
    pushdata(app, '/example/csv/file/Country', {
        'name': 'Lithuania',
        'flag': {
            '_id': 'file.txt',
            '_content_type': 'text/plain',
            '_content': base64.b64encode(b'DATA').decode(),
        },
    })

    assert parse_csv(app.get('/example/csv/file/Country/:format/csv?select(name, flag)')) == [
        ['name', 'flag._id', 'flag._content_type'],
        ['Lithuania', 'file.txt', 'text/plain'],
    ]


def test_csv_empty_ref(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property | type   | ref     | access
    example/csv/ref          |        |         |
      |   |   | Country      |        | name    |
      |   |   |   | name     | string |         | open
      |   |   | City         |        | name    |
      |   |   |   | name     | string |         | open
      |   |   |   | country  | ref    | Country | open
    ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authorize(['spinta_set_meta_fields'])
    app.authmodel('example/csv/ref', ['insert', 'search'])

    # Add data
    pushdata(app, '/example/csv/ref/City', {
        'name': 'Vilnius'
    })

    assert parse_csv(app.get('/example/csv/ref/City/:format/csv?select(name,country)')) == [
        ['name', 'country._id'],
        ['Vilnius', ''],
    ]


def test_csv_mixed_ref(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property | type   | ref     | access
    example/csv/ref          |        |         |
      |   |   | Country      |        | name    |
      |   |   |   | name     | string |         | open
      |   |   | City         |        | name    |
      |   |   |   | name     | string |         | open
      |   |   |   | country  | ref    | Country | open
    ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authorize(['spinta_set_meta_fields'])
    app.authmodel('example/csv/ref', ['insert', 'search'])

    # Add data
    lithuania_id = "8b607509-4413-459c-b13e-b4e52a38d024"
    pushdata(app, '/example/csv/ref/Country', {
        '_id': lithuania_id,
        'name': 'Lithuania',
    })
    pushdata(app, '/example/csv/ref/City', {
        'name': 'Vilnius',
        'country': {
            '_id': lithuania_id
        }
    })
    pushdata(app, '/example/csv/ref/City', {
        'name': 'Ryga',
    })

    assert parse_csv(app.get('/example/csv/ref/City/:format/csv?select(name,country)')) == [
        ['name', 'country._id'],
        ['Ryga', ''],
        ['Vilnius', lithuania_id],
    ]
