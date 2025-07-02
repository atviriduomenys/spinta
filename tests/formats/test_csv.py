import base64
from pathlib import Path
import pytest
from _pytest.fixtures import FixtureRequest
from starlette.datastructures import Headers

from spinta.core.config import RawConfig
from spinta.testing.client import TestClient
from spinta.testing.client import create_test_client
from spinta.testing.manifest import bootstrap_manifest
from spinta.testing.csv import parse_csv
from spinta.testing.data import pushdata, encode_page_values_manually


def test_export_csv(app):
    app.authorize(['spinta_set_meta_fields'])
    app.authmodel('datasets/csv/Country', [
        'insert',
        'patch',
        'getall',
        'search',
        'changes',
    ])

    resp = app.post('/datasets/csv/Country', json={'_data': [
        {
            '_op': 'insert',
            '_type': 'datasets/csv/Country',
            'code': 'lt',
            'title': 'Lithuania',
        },
        {
            '_op': 'insert',
            '_type': 'datasets/csv/Country',
            'code': 'lv',
            'title': 'LATVIA',
        },
    ]})
    assert resp.status_code == 200, resp.json()
    data = resp.json()['_data']
    lv = data[1]
    resp = app.patch(f'/datasets/csv/Country/{lv["_id"]}/', json={
        '_revision': lv['_revision'],
        'title': 'Latvia',
    })
    assert resp.status_code == 200, resp.json()

    assert app.get(
        '/datasets/csv/Country/:format/csv?select(code,title)&sort(+code)'
    ).text == (
        'code,title\r\n'
        'lt,Lithuania\r\n'
        'lv,Latvia\r\n'
    )

    resp = app.get('/datasets/csv/Country/:changes/:format/csv')
    assert resp.status_code == 200
    assert resp.headers['content-disposition'] == 'attachment; filename="Country.csv"'
    header, *lines = resp.text.splitlines()
    header = header.split(',')
    assert header == [
        '_cid',
        '_created',
        '_op',
        '_id',
        '_txn',
        '_revision',
        '_same_as',
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
    app.authmodel('Country', ['insert', 'search', ])
    resp = app.post('/Country', json={'_data': [
        {'_op': 'insert', '_type': 'Country', 'code': 'lt', 'title': 'Lithuania'},
        {'_op': 'insert', '_type': 'Country', 'code': 'lv', 'title': 'Latvia'},
        {'_op': 'insert', '_type': 'Country', 'code': 'ee', 'title': 'Estonia'},
    ]})
    assert resp.status_code == 200, resp.json()

    resp = app.get('/Country/:format/csv?select(code,title)&sort(code)&limit(1)')
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


@pytest.mark.manifests('internal_sql', 'csv')
def test_csv_file_dtype(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc, '''
    d | r | b | m | property | type   | ref  | access
    example/csv/file         |        |      |
      |   |   | Country      |        | name |
      |   |   |   | name     | string |      | open
      |   |   |   | flag     | file   |      | open
    ''',
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True
    )
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


@pytest.mark.manifests('internal_sql', 'csv')
def test_csv_empty_ref(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc, '''
    d | r | b | m | property | type   | ref     | access
    example/csv/ref          |        |         |
      |   |   | Country      |        | name    |
      |   |   |   | name     | string |         | open
      |   |   | City         |        | name    |
      |   |   |   | name     | string |         | open
      |   |   |   | country  | ref    | Country | open
    ''',
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True
    )
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


@pytest.mark.manifests('internal_sql', 'csv')
def test_csv_mixed_ref(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc, '''
    d | r | b | m | property | type   | ref     | access
    example/csv/ref          |        |         |
      |   |   | Country      |        | name    |
      |   |   |   | name     | string |         | open
      |   |   | City         |        | name    |
      |   |   |   | name     | string |         | open
      |   |   |   | country  | ref    | Country | open
    ''',
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True
    )
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


def test_csv_last_page(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property | type   | ref     | access
    example/csv/page         |        |         |
      |   |   | City         |        | name    |
      |   |   |   | name     | string |         | open
    ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authorize(['spinta_set_meta_fields'])
    app.authmodel('example/csv/page', ['insert', 'search'])

    # Add data
    result = pushdata(app, '/example/csv/page/City', {
        'name': 'Vilnius'
    })
    pushdata(app, '/example/csv/page/City', {
        'name': 'Kaunas'
    })

    assert parse_csv(app.get('/example/csv/page/City/:format/csv?select(name,_page)')) == [
        ['name', '_page.next'],
        ['Kaunas', ''],
        ['Vilnius', encode_page_values_manually({'name': 'Vilnius', '_id': result['_id']})],
    ]


@pytest.mark.manifests('internal_sql', 'csv')
def test_csv_text(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc, '''
    d | r | b | m | property | type    | ref     | access  | level | uri
    example/csv/text         |         |         |         |       | 
      |   |   |   |          | prefix  | rdf     |         |       | http://www.rdf.com
      |   |   |   |          |         | pav     |         |       | http://purl.org/pav/
      |   |   |   |          |         | dcat    |         |       | http://www.dcat.com
      |   |   |   |          |         | dct     |         |       | http://dct.com
      |   |   | Country      |         | name    |         |       | 
      |   |   |   | id       | integer |         |         |       |
      |   |   |   | name     | text    |         | open    | 3     |
      |   |   |   | name@en  | string  |         | open    |       |
      |   |   |   | name@lt  | string  |         | open    |       |
    ''',
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True
    )
    app = create_test_client(context)
    app.authmodel('example/csv', ['insert', 'getall', 'search'])

    pushdata(app, f'/example/csv/text/Country', {
        'id': 0,
        'name': {
            'lt': 'Lietuva',
            'en': 'Lithuania',
            'C': 'LT'
        }
    })
    pushdata(app, f'/example/csv/text/Country', {
        'id': 1,
        'name': {
            'lt': 'Anglija',
            'en': 'England',
            'C': 'UK'
        }
    })

    assert parse_csv(app.get("/example/csv/text/Country/:format/csv?select(id,name)&sort(id)", headers=Headers(headers={
        'accept-language': 'lt'
    }))) == [
        ['id', 'name'],
        ['0', 'Lietuva'],
        ['1', 'Anglija'],
    ]


@pytest.mark.manifests('internal_sql', 'csv')
def test_csv_text_with_lang(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc, '''
    d | r | b | m | property | type    | ref     | access  | level | uri
    example/csv/text/lang    |         |         |         |       | 
      |   |   |   |          | prefix  | rdf     |         |       | http://www.rdf.com
      |   |   |   |          |         | pav     |         |       | http://purl.org/pav/
      |   |   |   |          |         | dcat    |         |       | http://www.dcat.com
      |   |   |   |          |         | dct     |         |       | http://dct.com
      |   |   | Country      |         | name    |         |       | 
      |   |   |   | id       | integer |         |         |       |
      |   |   |   | name     | text    |         | open    | 3     |
      |   |   |   | name@en  | string  |         | open    |       |
      |   |   |   | name@lt  | string  |         | open    |       |
    ''',
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True
    )
    app = create_test_client(context)
    app.authmodel('example/csv', ['insert', 'getall', 'search'])

    pushdata(app, f'/example/csv/text/lang/Country', {
        'id': 0,
        'name': {
            'lt': 'Lietuva',
            'en': 'Lithuania',
            'C': 'LT'
        }
    })
    pushdata(app, f'/example/csv/text/lang/Country', {
        'id': 1,
        'name': {
            'lt': 'Anglija',
            'en': 'England',
            'C': 'UK'
        }
    })

    assert parse_csv(app.get("/example/csv/text/lang/Country/:format/csv?lang(*)&select(id,name)&sort(id)", headers=Headers(headers={
        'accept-language': 'lt'
    }))) == [
        ['id', 'name@C', 'name@en', 'name@lt'],
        ['0', 'LT', 'Lithuania', 'Lietuva'],
        ['1', 'UK', 'England', 'Anglija'],
    ]

    assert parse_csv(app.get("/example/csv/text/lang/Country/:format/csv?lang(en)&select(id,name)&sort(id)", headers=Headers(headers={
        'accept-language': 'lt'
    }))) == [
        ['id', 'name'],
        ['0', 'Lithuania'],
        ['1', 'England'],
    ]

    assert parse_csv(app.get("/example/csv/text/lang/Country/:format/csv?lang(en,lt)&select(id,name)&sort(id)", headers=Headers(headers={
        'accept-language': 'lt'
    }))) == [
        ['id', 'name@en', 'name@lt'],
        ['0', 'Lithuania', 'Lietuva'],
        ['1', 'England', 'Anglija'],
    ]


@pytest.mark.manifests('internal_sql', 'csv')
def test_csv_changes_text(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc, '''
    d | r | b | m | property | type    | ref     | access  | level | uri
    example/csv/text/changes |         |         |         |       | 
      |   |   |   |          | prefix  | rdf     |         |       | http://www.rdf.com
      |   |   |   |          |         | pav     |         |       | http://purl.org/pav/
      |   |   |   |          |         | dcat    |         |       | http://www.dcat.com
      |   |   |   |          |         | dct     |         |       | http://dct.com
      |   |   | Country      |         | name    |         |       | 
      |   |   |   | id       | integer |         |         |       |
      |   |   |   | name     | text    |         | open    | 3     |
      |   |   |   | name@en  | string  |         | open    |       |
      |   |   |   | name@lt  | string  |         | open    |       |
    ''',
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True
    )
    app = create_test_client(context)
    app.authmodel('example/csv', ['insert', 'getall', 'search', 'changes'])

    pushdata(app, f'/example/csv/text/changes/Country', {
        'id': 0,
        'name': {
            'lt': 'Lietuva',
            'en': 'Lithuania',
            'C': 'LT'
        }
    })
    pushdata(app, f'/example/csv/text/changes/Country', {
        'id': 1,
        'name': {
            'lt': 'Anglija',
            'en': 'England',
            'C': 'UK'
        }
    })

    assert parse_csv(app.get("/example/csv/text/changes/Country/:changes/-10/:format/csv?select(id,name)", headers=Headers(headers={
        'accept-language': 'lt'
    }))) == [
        ['id', 'name@C', 'name@en', 'name@lt'],
        ['0', 'LT', 'Lithuania', 'Lietuva'],
        ['1', 'UK', 'England', 'Anglija'],
    ]


@pytest.mark.manifests('internal_sql', 'csv')
def test_csv_empty(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc, '''
    d | r | b | m | property | type    | ref     | access  | level | uri
    example/csv/empty        |         |         |         |       | 
      |   |   |   |          | prefix  | rdf     |         |       | http://www.rdf.com
      |   |   |   |          |         | pav     |         |       | http://purl.org/pav/
      |   |   |   |          |         | dcat    |         |       | http://www.dcat.com
      |   |   |   |          |         | dct     |         |       | http://dct.com
      |   |   | Country      |         | name    |         |       | 
      |   |   |   | id       | integer |         |         |       |
      |   |   |   | name     | string  |         | open    | 3     |
    ''',
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True
    )
    app = create_test_client(context)
    app.authmodel('example/csv', ['insert', 'getall', 'search', 'changes'])

    assert parse_csv(app.get("/example/csv/empty/Country/:format/csv?select(id,name)")) == [
        ['id', 'name'],
    ]
