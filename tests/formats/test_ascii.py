import base64
import datetime
import hashlib
from pathlib import Path

import pytest
from _pytest.fixtures import FixtureRequest

from spinta import commands
from spinta.auth import AdminToken
from spinta.components import Action
from spinta.components import UrlParams
from spinta.core.config import RawConfig
from spinta.testing.manifest import load_manifest_and_context
from spinta.testing.data import pushdata
from spinta.testing.manifest import bootstrap_manifest
from spinta.testing.client import create_test_client
from spinta.testing.request import render_data


def sha1(s):
    return hashlib.sha1(s.encode()).hexdigest()


@pytest.mark.skip('datasets')
def test_export_ascii(app, mocker):
    mocker.patch(
        'spinta.backends.postgresql.dataset.utcnow',
        return_value=datetime.datetime(2019, 3, 6, 16, 15, 0, 816308),
    )

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
        '----  ---------\n'
        'code  title    \n'
        'lt    Lithuania\n'
        'lv    Latvia\n'
        '----  ---------\n'
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


@pytest.mark.manifests('internal_sql', 'csv')
@pytest.mark.asyncio
async def test_export_multiple_types(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig):
    context, manifest = load_manifest_and_context(rc, '''
    d | r | b | m | property | type    | ref   | access
    example                  |         |       |
      |   |   | A            |         | value |
      |   |   |   | value    | integer |       | open
      |   |   | B            |         | value |
      |   |   |   | value    | integer |       | open
      |   |   | C            |         | value |
      |   |   |   | value    | integer |       | open

    ''', manifest_type=manifest_type, tmp_path=tmp_path)
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
    ns = commands.get_namespace(context, manifest, '')
    params = UrlParams()
    assert ''.join(exporter(context, ns, Action.GETALL, params, rows)) == (
        '\n'
        '\n'
        'Table: example/A\n'
        '---------  ---  ---------  -----  -----\n'
        '_type      _id  _revision  _page  value\n'
        'example/A  ∅    ∅          ∅      1\n'
        'example/A  ∅    ∅          ∅      2\n'
        'example/A  ∅    ∅          ∅      3\n'
        '---------  ---  ---------  -----  -----\n'
        '\n'
        '\n'
        'Table: example/B\n'
        '---------  ---  ---------  -----  -----\n'
        '_type      _id  _revision  _page  value\n'
        'example/B  ∅    ∅          ∅      1\n'
        'example/B  ∅    ∅          ∅      2\n'
        'example/B  ∅    ∅          ∅      3\n'
        '---------  ---  ---------  -----  -----\n'
        '\n'
        '\n'
        'Table: example/C\n'
        '---------  ---  ---------  -----  -----\n'
        '_type      _id  _revision  _page  value\n'
        'example/C  ∅    ∅          ∅      1\n'
        'example/C  ∅    ∅          ∅      2\n'
        'example/C  ∅    ∅          ∅      3\n'
        '---------  ---  ---------  -----  -----\n'
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
        '----  ---------\n'
        'code  title    \n'
        'lt    Lithuania\n'
        'lv    Latvia\n'
        '----  ---------\n'
    )


@pytest.mark.manifests('internal_sql', 'csv')
def test_ascii_ref_dtype(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc, '''
    d | r | b | m | property | type   | ref     | access
    example/ascii/ref        |        |         |
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
    app.authmodel('example/ascii/ref', ['insert', 'search'])

    # Add data
    country = pushdata(app, '/example/ascii/ref/Country', {'name': 'Lithuania'})
    pushdata(app, '/example/ascii/ref/City', {
        'name': 'Vilnius',
        'country': {'_id': country['_id']},
    })

    assert app.get('/example/ascii/ref/City/:format/ascii?select(name, country)').text == (
        '-------  ------------------------------------\n'
        'name     country._id                         \n'
        f'Vilnius  {country["_id"]}\n'
        '-------  ------------------------------------\n'
    )


@pytest.mark.manifests('internal_sql', 'csv')
def test_ascii_file_dtype(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc, '''
    d | r | b | m | property | type   | ref  | access
    example/ascii/file       |        |      |
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
        '---------  --------  ------------------\n'
        'name       flag._id  flag._content_type\n'
        'Lithuania  file.txt  text/plain\n'
        '---------  --------  ------------------\n'
    )


@pytest.mark.manifests('internal_sql', 'csv')
@pytest.mark.asyncio
async def test_ascii_getone(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
):
    context, manifest = load_manifest_and_context(rc, '''
    d | r | b | m | property | type   | ref  | access
    example                  |        |      |
      |   |   | City         |        | name |
      |   |   |   | name     | string |      | open
    ''', manifest_type=manifest_type, tmp_path=tmp_path)

    _id = '19e4f199-93c5-40e5-b04e-a575e81ac373'
    result = render_data(
        context, manifest,
        f'example/City/{_id}', None,
        accept='text/plain',
        data={
            '_type': 'example/City',
            '_id': _id,
            '_revision': 'b6197bb7-3592-4cdb-a61c-5a618f44950c',
            'name': 'Vilnius',
        }
    )
    result = ''.join([x async for x in result.body_iterator]).splitlines()
    assert result == [
        '',
        '',
        'Table: example/City',
        '------------  ------------------------------------  ------------------------------------  -----  -------',
        '_type         _id                                   _revision                             _page  name   ',
        'example/City  19e4f199-93c5-40e5-b04e-a575e81ac373  b6197bb7-3592-4cdb-a61c-5a618f44950c  ∅      Vilnius',
        '------------  ------------------------------------  ------------------------------------  -----  -------',
    ]


@pytest.mark.manifests('internal_sql', 'csv')
def test_ascii_params(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc, '''
    d | r | b | m | property   | type    | ref  | access
    example/ascii/params       |         |      |
      |   |   | Country        |         | name |
      |   |   |   | name       | string  |      | open
      |   |   |   | capital    | string  |      | open
    ''',
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True
    )
    app = create_test_client(context)
    app.authmodel('example/ascii/params', ['insert', 'search'])

    # Add data
    pushdata(app, '/example/ascii/params/Country', {
        'name': 'Lithuania',
        'capital': 'Vilnius'
    })

    assert app.get('/example/ascii/params/Country/:format/ascii?select(name,capital)&format(width(15))').text == (
        '---------  ...\n'
        'name       ...\n'
        'Lithuania  ...\n'
        '---------  ...\n'
    )

    assert app.get('/example/ascii/params/Country/:format/ascii?select(name,capital)&format(colwidth(7))').text == (
        '-------  -------\n'
        'name     capital\n'
        'Lithuan  Vilnius\\\n'
        'ia\n'
        '-------  -------\n'
    )

    assert app.get('/example/ascii/params/Country/:format/ascii?select(name,capital)&format(vlen(7))').text == (
        '----------  -------\n'
        'name        capital\n'
        'Lithuan...  Vilnius\n'
        '----------  -------\n'
    )


@pytest.mark.manifests('internal_sql', 'csv')
def test_ascii_multiline(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc, '''
    d | r | b | m | property   | type    | ref  | access
    example/ascii/params       |         |      |
      |   |   | Country        |         | name |
      |   |   |   | name       | string  |      | open
      |   |   |   | capital    | string  |      | open
    ''',
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True
    )
    app = create_test_client(context)
    app.authmodel('example/ascii/params', ['insert', 'search'])

    # Add data
    pushdata(app, '/example/ascii/params/Country', {
        'name': 'Lithuania',
        'capital': 'Current capital - Vilnius.\nPrevious - Kernave.'
    })

    assert app.get('/example/ascii/params/Country/:format/ascii?select(name,capital)&format(colwidth(20))').text == (
        '---------  ----------------\n'
        'name       capital         \n'
        'Lithuania  Current capital \\\n'
        '           - Vilnius.      \\\n'
        '           Previous - Kerna\\\n'
        '           ve.\n'
        '---------  ----------------\n'
    )

