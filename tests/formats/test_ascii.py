import base64
import datetime
import hashlib

import pytest
from _pytest.fixtures import FixtureRequest

from spinta.auth import AdminToken
from spinta.components import Action
from spinta.components import UrlParams
from spinta.core.config import RawConfig
from spinta.testing.manifest import load_manifest_and_context
from spinta.testing.data import pushdata
from spinta.testing.manifest import bootstrap_manifest
from spinta.testing.client import create_test_client


def sha1(s):
    return hashlib.sha1(s.encode()).hexdigest()


@pytest.mark.skip('datasets')
def test_export_ascii(app, mocker):
    mocker.patch('spinta.backends.postgresql.dataset.utcnow', return_value=datetime.datetime(2019, 3, 6, 16, 15, 0, 816308))

    app.authorize(['spinta_set_meta_fields'])
    app.authmodel('country/:dataset/csv/:resource/countries', ['upsert', 'getall', 'search', 'changes'])

    resp = app.post('/country/:dataset/csv/:resource/countries', json={'_data': [
        {
            '_op': 'upsert',
            '_type': 'country/:dataset/csv/:resource/countries',
            '_id': sha1('1'),
            '_where': '_id="' + sha1('1') + '"',
            'code': 'lt',
            'title': 'Lithuania',
        },
        {
            '_op': 'upsert',
            '_type': 'country/:dataset/csv/:resource/countries',
            '_id': sha1('2'),
            '_where': '_id="' + sha1('2') + '"',
            'code': 'lv',
            'title': 'LATVIA',
        },
        {
            '_op': 'upsert',
            '_type': 'country/:dataset/csv/:resource/countries',
            '_id': sha1('2'),
            '_where': '_id="' + sha1('2') + '"',
            'code': 'lv',
            'title': 'Latvia',
        },
    ]})
    assert resp.status_code == 200, resp.json()
    assert app.get('/country/:dataset/csv/:resource/countries/:format/ascii?select(code,title)&sort(+code)&format(colwidth(42))').text == (
        'code     title  \n'
        '================\n'
        'lt     Lithuania\n'
        'lv     Latvia   '
    )

    resp = app.get('/country/:dataset/csv/:resource/countries/:changes')
    changes = resp.json()['_data']
    changes = [{k: str(v) for k, v in row.items()} for row in changes]
    res = app.get('country/:dataset/csv/:resource/countries/:changes/:format/ascii?format(colwidth(42))').text
    lines = res.splitlines()
    cols = lines[0].split()
    data = [dict(zip(cols, [v.strip() for v in row.split()])) for row in lines[2:]]
    data = [{k: v for k, v in row.items() if v != 'None'} for row in data]
    assert data == changes


@pytest.mark.asyncio
async def test_export_multiple_types(rc: RawConfig):
    context, manifest = load_manifest_and_context(rc, '''
    d | r | b | m | property | type    | ref   | access
    example                  |         |       |
      |   |   | A            |         | value |
      |   |   |   | value    | integer |       | open
      |   |   | B            |         | value |
      |   |   |   | value    | integer |       | open
      |   |   | C            |         | value |
      |   |   |   | value    | integer |       | open

    ''')
    rows = [
        {'_type': 'example/A', 'value': 1},
        {'_type': 'example/A', 'value': 2},
        {'_type': 'example/A', 'value': 3},
        {'_type': 'example/B', 'value': 1},
        {'_type': 'example/B', 'value': 2},
        {'_type': 'example/B', 'value': 3},
        {'_type': 'example/C', 'value': 1},
        {'_type': 'example/C', 'value': 2},
        {'_type': 'example/C', 'value': 3},
    ]
    context.set('auth.token', AdminToken())
    config = context.get('config')
    exporter = config.exporters['ascii']
    ns = manifest.namespaces['']
    params = UrlParams()
    assert ''.join(exporter(context, ns, Action.GETALL, params, rows)) == (
        '\n'
        '\n'
        'Table: example/A\n'
        '  _type     _id    _revision   value\n'
        '====================================\n'
        'example/A   None   None        1    \n'
        'example/A   None   None        2    \n'
        'example/A   None   None        3    \n'
        '\n'
        'Table: example/B\n'
        '  _type     _id    _revision   value\n'
        '====================================\n'
        'example/B   None   None        1    \n'
        'example/B   None   None        2    \n'
        'example/B   None   None        3    \n'
        '\n'
        'Table: example/C\n'
        '  _type     _id    _revision   value\n'
        '====================================\n'
        'example/C   None   None        1    \n'
        'example/C   None   None        2    \n'
        'example/C   None   None        3    '
    )


@pytest.mark.skip('datasets')
def test_export_ascii_params(app, mocker):
    app.authmodel('country/:dataset/csv/:resource/countries', ['insert', 'search'])
    resp = app.post('/country/:dataset/csv/:resource/countries', json={'_data': [
        {
            '_op': 'insert',
            '_type': 'country/:dataset/csv/:resource/countries',
            'code': 'lt',
            'title': 'Lithuania',
        },
        {
            '_op': 'insert',
            '_type': 'country/:dataset/csv/:resource/countries',
            'code': 'lv',
            'title': 'Latvia',
        },
    ]})
    assert resp.status_code == 200, resp.json()
    assert app.get('/country/:dataset/csv/:resource/countries/:format/ascii?select(code,title)&sort(+code)&format(width(50))').text == (
        'code     title  \n'
        '================\n'
        'lt     Lithuania\n'
        'lv     Latvia   '
    )


def test_ascii_ref_dtype(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property | type   | ref     | access
    example/ascii/ref        |        |         |
      |   |   | Country      |        | name    |
      |   |   |   | name     | string |         | open
      |   |   | City         |        | name    |
      |   |   |   | name     | string |         | open
      |   |   |   | country  | ref    | Country | open
    ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authmodel('example/ascii/ref', ['insert', 'search'])

    # Add data
    country = pushdata(app, '/example/ascii/ref/Country', {'name': 'Lithuania'})
    pushdata(app, '/example/ascii/ref/City', {
        'name': 'Vilnius',
        'country': {'_id': country['_id']},
    })

    assert app.get('/example/ascii/ref/City/:format/ascii?select(name, country)').text == (
        ' name                 country._id             \n'
        '==============================================\n'
        f'Vilnius   {country["_id"]}'
    )


def test_ascii_file_dtype(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property | type   | ref  | access
    example/ascii/file       |        |      |
      |   |   | Country      |        | name |
      |   |   |   | name     | string |      | open
      |   |   |   | flag     | file   |      | open
    ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authmodel('example/ascii/file', ['insert', 'search'])

    # Add data
    pushdata(app, '/example/ascii/file/Country', {
        'name': 'Lithuania',
        'flag': {
            '_id': 'file.txt',
            '_content_type': 'text/plain',
            '_content': base64.b64encode(b'DATA').decode(),
        },
    })

    assert app.get('/example/ascii/file/Country/:format/ascii?select(name, flag)').text == (
        '  name      flag._id   flag._content_type\n'
        '=========================================\n'
        'Lithuania   file.txt   text/plain        '
    )
