import datetime
import pathlib

import pytest

import sqlalchemy as sa

from spinta.core.config import RawConfig
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.data import listdata
from spinta.testing.client import create_test_client
from spinta.testing.context import create_test_context
from spinta.testing.datasets import Sqlite
from spinta.testing.datasets import create_sqlite_db
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.tabular import create_tabular_manifest
from spinta.testing.utils import error
from spinta.testing.client import create_remote_server
from spinta.utils.schema import NA


@pytest.fixture(scope='module')
def geodb():
    with create_sqlite_db({
        'salis': [
            sa.Column('id', sa.Integer, primary_key=True),
            sa.Column('kodas', sa.Text),
            sa.Column('pavadinimas', sa.Text),
        ],
        'miestas': [
            sa.Column('id', sa.Integer, primary_key=True),
            sa.Column('pavadinimas', sa.Text),
            sa.Column('salis', sa.Text),
        ],
    }) as db:
        db.write('salis', [
            {'kodas': 'lt', 'pavadinimas': 'Lietuva'},
            {'kodas': 'lv', 'pavadinimas': 'Latvija'},
            {'kodas': 'ee', 'pavadinimas': 'Estija'},
        ])
        db.write('miestas', [
            {'salis': 'lt', 'pavadinimas': 'Vilnius'},
            {'salis': 'lv', 'pavadinimas': 'Ryga'},
            {'salis': 'ee', 'pavadinimas': 'Talinas'},
        ])
        yield db


def create_rc(rc: RawConfig, tmpdir: pathlib.Path, db: Sqlite) -> RawConfig:
    return rc.fork({
        'manifests': {
            'default': {
                'type': 'tabular',
                'path': str(tmpdir / 'manifest.csv'),
                'backend': 'sql',
                'keymap': 'default',
            },
        },
        'backends': {
            'sql': {
                'type': 'sql',
                'dsn': db.dsn,
            },
        },
        # tests/config/clients/3388ea36-4a4f-4821-900a-b574c8829d52.yml
        'default_auth_client': '3388ea36-4a4f-4821-900a-b574c8829d52',
    })


def configure_remote_server(
    cli,
    local_rc: RawConfig,
    rc: RawConfig,
    tmpdir: pathlib.Path,
    responses,
):
    cli.invoke(local_rc, [
        'copy',
        '--no-source',
        '--access', 'open',
        '-o', tmpdir / 'remote.csv',
        tmpdir / 'manifest.csv',
    ])

    # Create remote server with PostgreSQL backend
    tmpdir = pathlib.Path(tmpdir)
    remote_rc = rc.fork({
        'manifests': {
            'default': {
                'type': 'tabular',
                'path': str(tmpdir / 'remote.csv'),
                'backend': 'default',
            },
        },
        'backends': ['default'],
    })
    return create_remote_server(
        remote_rc,
        tmpdir,
        responses,
        scopes=['spinta_set_meta_fields', 'spinta_upsert'],
        credsfile=True,
    )


def create_client(rc: RawConfig, tmpdir: pathlib.Path, geodb: Sqlite):
    rc = create_rc(rc, tmpdir, geodb)
    context = create_test_context(rc)
    return create_test_client(context)


def test_filter(rc, tmpdir, geodb):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    id | d | r | b | m | property | source      | prepare   | type   | ref     | level | access | uri | title   | description
       | datasets/gov/example     |             |           |        |         |       |        |     | Example |
       |   | data                 |             |           | sql    |         |       |        |     | Data    |
       |   |   |                  |             |           |        |         |       |        |     |         |
       |   |   |   | country      | salis       | code='lt' |        | code    |       |        |     | Country |
       |   |   |   |   | code     | kodas       |           | string |         | 3     | open   |     | Code    |
       |   |   |   |   | name     | pavadinimas |           | string |         | 3     | open   |     | Name    |
    '''))

    app = create_client(rc, tmpdir, geodb)

    resp = app.get('/datasets/gov/example/country')
    assert listdata(resp, 'code', 'name') == [
        ('lt', 'Lietuva'),
    ]


def test_filter_join(rc, tmpdir, geodb):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    id | d | r | b | m | property | source      | prepare           | type   | ref     | level | access | uri | title   | description
       | datasets/gov/example     |             |                   |        |         |       |        |     | Example |
       |   | data                 |             |                   | sql    |         |       |        |     | Data    |
       |   |   |                  |             |                   |        |         |       |        |     |         |
       |   |   |   | country      | salis       |                   |        | code    |       |        |     | Country |
       |   |   |   |   | code     | kodas       |                   | string |         | 3     | open   |     | Code    |
       |   |   |   |   | name     | pavadinimas |                   | string |         | 3     | open   |     | Name    |
       |   |   |                  |             |                   |        |         |       |        |     |         |
       |   |   |   | city         | miestas     | country.code='lt' |        | name    |       |        |     | City    |
       |   |   |   |   | name     | pavadinimas |                   | string |         | 3     | open   |     | Name    |
       |   |   |   |   | country  | salis       |                   | ref    | country | 4     | open   |     | Country |
    '''))

    app = create_client(rc, tmpdir, geodb)

    resp = app.get('/datasets/gov/example/country')
    codes = dict(listdata(resp, '_id', 'code'))

    resp = app.get('/datasets/gov/example/city?sort(name)')
    data = listdata(resp, 'country._id', 'name')
    data = [(codes.get(country), city) for country, city in data]
    assert data == [
        ('lt', 'Vilnius'),
    ]


def test_filter_join_array_value(rc, tmpdir, geodb):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    id | d | r | b | m | property | source      | prepare                  | type   | ref     | level | access | uri | title   | description
       | datasets/gov/example     |             |                          |        |         |       |        |     | Example |
       |   | data                 |             |                          | sql    |         |       |        |     | Data    |
       |   |   |                  |             |                          |        |         |       |        |     |         |
       |   |   |   | country      | salis       |                          |        | code    |       |        |     | Country |
       |   |   |   |   | code     | kodas       |                          | string |         | 3     | open   |     | Code    |
       |   |   |   |   | name     | pavadinimas |                          | string |         | 3     | open   |     | Name    |
       |   |   |                  |             |                          |        |         |       |        |     |         |
       |   |   |   | city         | miestas     | country.code=['lt','lv'] |        | name    |       |        |     | City    |
       |   |   |   |   | name     | pavadinimas |                          | string |         | 3     | open   |     | Name    |
       |   |   |   |   | country  | salis       |                          | ref    | country | 4     | open   |     | Country |
    '''))

    app = create_client(rc, tmpdir, geodb)

    resp = app.get('/datasets/gov/example/country')
    codes = dict(listdata(resp, '_id', 'code'))

    resp = app.get('/datasets/gov/example/city?sort(name)')
    data = listdata(resp, 'country._id', 'name', sort='name')
    data = [(codes.get(country), city) for country, city in data]
    assert data == [
        ('lv', 'Ryga'),
        ('lt', 'Vilnius'),
    ]


def test_filter_join_ne_array_value(rc, tmpdir, geodb):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    id | d | r | b | m | property | source      | prepare                   | type   | ref     | level | access | uri | title   | description
       | datasets/gov/example     |             |                           |        |         |       |        |     | Example |
       |   | data                 |             |                           | sql    |         |       |        |     | Data    |
       |   |   |                  |             |                           |        |         |       |        |     |         |
       |   |   |   | country      | salis       |                           |        | code    |       |        |     | Country |
       |   |   |   |   | code     | kodas       |                           | string |         | 3     | open   |     | Code    |
       |   |   |   |   | name     | pavadinimas |                           | string |         | 3     | open   |     | Name    |
       |   |   |                  |             |                           |        |         |       |        |     |         |
       |   |   |   | city         | miestas     | country.code!=['lt','lv'] |        | name    |       |        |     | City    |
       |   |   |   |   | name     | pavadinimas |                           | string |         | 3     | open   |     | Name    |
       |   |   |   |   | country  | salis       |                           | ref    | country | 4     | open   |     | Country |
    '''))

    app = create_client(rc, tmpdir, geodb)

    resp = app.get('/datasets/gov/example/country')
    codes = dict(listdata(resp, '_id', 'code'))

    resp = app.get('/datasets/gov/example/city?sort(name)')
    data = listdata(resp, 'country._id', 'name', sort='name')
    data = [(codes.get(country), city) for country, city in data]
    assert data == [
        ('ee', 'Talinas'),
    ]


@pytest.mark.skip('todo')
def test_filter_multi_column_pk(rc, tmpdir, geodb):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    id | d | r | b | m | property | source      | prepare            | type   | ref           | level | access | uri | title   | description
       | datasets/gov/example     |             |                    |        |               |       |        |     | Example |
       |   | data                 |             |                    | sql    |               |       |        |     | Data    |
       |   |   |                  |             |                    |        |               |       |        |     |         |
       |   |   |   | country      | salis       |                    |        | id,code       |       |        |     | Country |
       |   |   |   |   | id       | id          |                    | string |               | 3     | open   |     | Code    |
       |   |   |   |   | code     | kodas       |                    | string |               | 3     | open   |     | Code    |
       |   |   |   |   | name     | pavadinimas |                    | string |               | 3     | open   |     | Name    |
       |   |   |                  |             |                    |        |               |       |        |     |         |
       |   |   |   | city         | miestas     | country.code!='ee' |        | name          |       |        |     | City    |
       |   |   |   |   | name     | pavadinimas |                    | string |               | 3     | open   |     | Name    |
       |   |   |   |   | country  | salis       |                    | ref    | country[code] | 4     | open   |     | Country |
    '''))

    app = create_client(rc, tmpdir, geodb)

    resp = app.get('/datasets/gov/example/country')
    codes = dict(listdata(resp, '_id', 'code'))

    resp = app.get('/datasets/gov/example/city?sort(name)')
    data = listdata(resp, 'country._id', 'name', sort='name')
    data = [(codes.get(country), city) for country, city in data]
    assert data == [
        ('lv', 'Ryga'),
        ('lt', 'Vilnius'),
    ]


def test_getall(rc, tmpdir, geodb):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    id | d | r | b | m | property | source      | prepare | type   | ref     | level | access | uri | title   | description
       | datasets/gov/example     |             |         |        |         |       |        |     | Example |
       |   | data                 |             |         | sql    |         |       |        |     | Data    |
       |   |   |                  |             |         |        |         |       |        |     |         |
       |   |   |   | country      | salis       |         |        | code    |       |        |     | Country |
       |   |   |   |   | code     | kodas       |         | string |         | 3     | open   |     | Code    |
       |   |   |   |   | name     | pavadinimas |         | string |         | 3     | open   |     | Name    |
       |   |   |                  |             |         |        |         |       |        |     |         |
       |   |   |   | city         | miestas     |         |        | name    |       |        |     | City    |
       |   |   |   |   | name     | pavadinimas |         | string |         | 3     | open   |     | Name    |
       |   |   |   |   | country  | salis       |         | ref    | country | 4     | open   |     | Country |
    '''))

    app = create_client(rc, tmpdir, geodb)

    resp = app.get('/datasets/gov/example/country?sort(code)')
    codes = dict(listdata(resp, '_id', 'code'))
    assert listdata(resp, 'code', 'name', '_type') == [
        ('ee', 'Estija', 'datasets/gov/example/country'),
        ('lt', 'Lietuva', 'datasets/gov/example/country'),
        ('lv', 'Latvija', 'datasets/gov/example/country'),
    ]

    resp = app.get('/datasets/gov/example/city?sort(name)')
    data = listdata(resp, 'country._id', 'name', '_type', sort='name')
    data = [(codes.get(country), city, _type) for country, city, _type in data]
    assert data == [
        ('lv', 'Ryga', 'datasets/gov/example/city'),
        ('ee', 'Talinas', 'datasets/gov/example/city'),
        ('lt', 'Vilnius', 'datasets/gov/example/city'),
    ]


def test_select(rc, tmpdir, geodb):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    id | d | r | b | m | property | source      | prepare | type   | ref     | level | access | uri | title   | description
       | datasets/gov/example     |             |         |        |         |       |        |     | Example |
       |   | data                 |             |         | sql    |         |       |        |     | Data    |
       |   |   |                  |             |         |        |         |       |        |     |         |
       |   |   |   | country      | salis       |         |        | code    |       |        |     | Country |
       |   |   |   |   | code     | kodas       |         | string |         | 3     | open   |     | Code    |
       |   |   |   |   | name     | pavadinimas |         | string |         | 3     | open   |     | Name    |
    '''))

    app = create_client(rc, tmpdir, geodb)

    resp = app.get('/datasets/gov/example/country?select(code,name)')
    assert listdata(resp, 'code', 'name') == [
        ('ee', 'Estija'),
        ('lt', 'Lietuva'),
        ('lv', 'Latvija'),
    ]


@pytest.mark.skip('TODO')
def test_select_len(rc, tmpdir, geodb):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    id | d | r | b | m | property | source      | prepare | type   | ref     | level | access | uri | title   | description
       | datasets/gov/example     |             |         |        |         |       |        |     | Example |
       |   | data                 |             |         | sql    |         |       |        |     | Data    |
       |   |   |                  |             |         |        |         |       |        |     |         |
       |   |   |   | country      | salis       |         |        | code    |       |        |     | Country |
       |   |   |   |   | code     | kodas       |         | string |         | 3     | open   |     | Code    |
       |   |   |   |   | name     | pavadinimas |         | string |         | 3     | open   |     | Name    |
    '''))

    app = create_client(rc, tmpdir, geodb)

    resp = app.get('/datasets/gov/example/country?select(code,len(name))')
    assert listdata(resp, 'code', 'len(name)') == [
        ('ee', 6),
        ('lt', 7),
        ('lv', 7),
    ]


def test_filter_len(rc, tmpdir, geodb):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    id | d | r | b | m | property | source      | prepare | type   | ref     | level | access | uri | title   | description
       | datasets/gov/example     |             |         |        |         |       |        |     | Example |
       |   | data                 |             |         | sql    |         |       |        |     | Data    |
       |   |   |                  |             |         |        |         |       |        |     |         |
       |   |   |   | country      | salis       |         |        | code    |       |        |     | Country |
       |   |   |   |   | code     | kodas       |         | string |         | 3     | open   |     | Code    |
       |   |   |   |   | name     | pavadinimas |         | string |         | 3     | open   |     | Name    |
    '''))

    app = create_client(rc, tmpdir, geodb)

    resp = app.get('/datasets/gov/example/country?select(code,name)&len(name)=7&sort(code)')
    assert listdata(resp, 'code', 'name') == [
        ('lt', 'Lietuva'),
        ('lv', 'Latvija'),
    ]


def test_private_property(rc, tmpdir, geodb):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    id | d | r | b | m | property | source      | prepare    | type   | ref     | level | access  | uri | title   | description
       | datasets/gov/example     |             |            |        |         |       |         |     | Example |
       |   | data                 |             |            | sql    |         |       |         |     | Data    |
       |   |   |                  |             |            |        |         |       |         |     |         |
       |   |   |   | country      | salis       | code!='ee' |        | code    |       |         |     | Country |
       |   |   |   |   | code     | kodas       |            | string |         | 3     | private |     | Code    |
       |   |   |   |   | name     | pavadinimas |            | string |         | 3     | open    |     | Name    |
    '''))

    app = create_client(rc, tmpdir, geodb)

    resp = app.get('/datasets/gov/example/country')
    assert listdata(resp) == [
        'Latvija',
        'Lietuva',
    ]


def test_all_private_properties(rc, tmpdir, geodb):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    id | d | r | b | m | property | source      | prepare    | type   | ref     | level | access  | uri | title   | description
       | datasets/gov/example     |             |            |        |         |       |         |     | Example |
       |   | data                 |             |            | sql    |         |       |         |     | Data    |
       |   |   |                  |             |            |        |         |       |         |     |         |
       |   |   |   | country      | salis       | code!='ee' |        | code    |       |         |     | Country |
       |   |   |   |   | code     | kodas       |            | string |         | 3     | private |     | Code    |
       |   |   |   |   | name     | pavadinimas |            | string |         | 3     | private |     | Name    |
    '''))

    app = create_client(rc, tmpdir, geodb)

    resp = app.get('/datasets/gov/example/country')
    assert error(resp, status=401) == 'AuthorizedClientsOnly'


def test_default_access(rc, tmpdir, geodb):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    id | d | r | b | m | property | source      | prepare    | type   | ref     | level | access  | uri | title   | description
       | datasets/gov/example     |             |            |        |         |       |         |     | Example |
       |   | data                 |             |            | sql    |         |       |         |     | Data    |
       |   |   |                  |             |            |        |         |       |         |     |         |
       |   |   |   | country      | salis       | code!='ee' |        | code    |       |         |     | Country |
       |   |   |   |   | code     | kodas       |            | string |         | 3     |         |     | Code    |
       |   |   |   |   | name     | pavadinimas |            | string |         | 3     |         |     | Name    |
    '''))

    app = create_client(rc, tmpdir, geodb)

    resp = app.get('/datasets/gov/example/country')
    assert error(resp, status=401) == 'AuthorizedClientsOnly'


def test_model_open_access(rc, tmpdir, geodb):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    id | d | r | b | m | property | source      | prepare    | type   | ref     | level | access  | uri | title   | description
       | datasets/gov/example     |             |            |        |         |       |         |     | Example |
       |   | data                 |             |            | sql    |         |       |         |     | Data    |
       |   |   |                  |             |            |        |         |       |         |     |         |
       |   |   |   | country      | salis       | code!='ee' |        | code    |       | open    |     | Country |
       |   |   |   |   | code     | kodas       |            | string |         | 3     |         |     | Code    |
       |   |   |   |   | name     | pavadinimas |            | string |         | 3     |         |     | Name    |
    '''))

    app = create_client(rc, tmpdir, geodb)

    resp = app.get('/datasets/gov/example/country')
    assert listdata(resp) == [
        ('lt', 'Lietuva'),
        ('lv', 'Latvija'),
    ]


def test_property_public_access(rc, tmpdir, geodb):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    id | d | r | b | m | property | source      | prepare    | type   | ref     | level | access  | uri | title   | description
       | datasets/gov/example     |             |            |        |         |       |         |     | Example |
       |   | data                 |             |            | sql    |         |       |         |     | Data    |
       |   |   |                  |             |            |        |         |       |         |     |         |
       |   |   |   | country      | salis       | code!='ee' |        | code    |       |         |     | Country |
       |   |   |   |   | code     | kodas       |            | string |         | 3     | public  |     | Code    |
       |   |   |   |   | name     | pavadinimas |            | string |         | 3     | open    |     | Name    |
    '''))

    app = create_client(rc, tmpdir, geodb)

    resp = app.get('/datasets/gov/example/country')
    assert listdata(resp) == [
        'Latvija',
        'Lietuva',
    ]

    resp = app.get('/datasets/gov/example/country', headers={'Accept': 'text/html'})
    assert listdata(resp) == [
        'Latvija',
        'Lietuva',
    ]


def test_select_protected_property(rc, tmpdir, geodb):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    id | d | r | b | m | property | source      | prepare    | type   | ref     | level | access  | uri | title   | description
       | datasets/gov/example     |             |            |        |         |       |         |     | Example |
       |   | data                 |             |            | sql    |         |       |         |     | Data    |
       |   |   |                  |             |            |        |         |       |         |     |         |
       |   |   |   | country      | salis       | code!='ee' |        | code    |       |         |     | Country |
       |   |   |   |   | code     | kodas       |            | string |         | 3     | public  |     | Code    |
       |   |   |   |   | name     | pavadinimas |            | string |         | 3     | open    |     | Name    |
    '''))

    app = create_client(rc, tmpdir, geodb)

    resp = app.get('/datasets/gov/example/country?select(code,name)')
    assert error(resp) == 'PropertyNotFound'

    resp = app.get('/datasets/gov/example/country?select(code,name)', headers={'Accept': 'text/html'})
    assert error(resp) == 'PropertyNotFound'


def test_ns_getall(rc, tmpdir, geodb):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    id | d | r | b | m | property | source      | prepare    | type   | ref     | level | access  | uri | title   | description
       | datasets/gov/example     |             |            |        |         |       |         |     | Example |
       |   | data                 |             |            | sql    |         |       |         |     | Data    |
       |   |   |                  |             |            |        |         |       |         |     |         |
       |   |   |   | country      | salis       | code!='ee' |        | code    |       |         |     | Country |
       |   |   |   |   | code     | kodas       |            | string |         | 3     | public  |     | Code    |
       |   |   |   |   | name     | pavadinimas |            | string |         | 3     | open    |     | Name    |
    '''))

    app = create_client(rc, tmpdir, geodb)

    resp = app.get('/datasets/gov/example')
    assert listdata(resp, '_id', 'title') == [
        ('datasets/gov/example/country', 'Country'),
    ]

    resp = app.get('/datasets/gov/example', headers={'Accept': 'text/html'})
    assert listdata(resp, 'title') == [
        '📄 Country',
    ]


def test_push(postgresql, rc, cli: SpintaCliRunner, responses, tmpdir, geodb, request):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    d | r | b | m | property| type   | ref     | source       | access
    datasets/gov/example    |        |         |              |
      | data                | sql    |         |              |
      |   |                 |        |         |              |
      |   |   | Country     |        | code    | salis        |
      |   |   |   | code    | string |         | kodas        | open
      |   |   |   | name    | string |         | pavadinimas  | open
      |   |                 |        |         |              |
      |   |   | City        |        | name    | miestas      |
      |   |   |   | name    | string |         | pavadinimas  | open
      |   |   |   | country | ref    | Country | salis        | open
    '''))

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmpdir, geodb)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmpdir, responses)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == 'https://example.com/'
    result = cli.invoke(localrc, [
        'push',
        '-d', 'datasets/gov/example',
        '-o', 'spinta+' + remote.url,
        '--credentials', remote.credsfile,
    ])
    assert result.exit_code == 0

    remote.app.authmodel('datasets/gov/example/Country', ['getall'])
    resp = remote.app.get('/datasets/gov/example/Country')
    assert listdata(resp, 'code', 'name') == [
        ('ee', 'Estija'),
        ('lt', 'Lietuva'),
        ('lv', 'Latvija')
    ]

    codes = dict(listdata(resp, '_id', 'code'))
    remote.app.authmodel('datasets/gov/example/City', ['getall'])
    resp = remote.app.get('/datasets/gov/example/City')
    data = listdata(resp, 'country._id', 'name')
    data = [(codes.get(country), city) for country, city in data]
    assert sorted(data) == [
        ('ee', 'Talinas'),
        ('lt', 'Vilnius'),
        ('lv', 'Ryga'),
    ]

    # Add new data to local server
    geodb.write('miestas', [
        {'salis': 'lt', 'pavadinimas': 'Kaunas'},
    ])

    # Push data from local to remote.
    cli.invoke(localrc, [
        'push',
        '-d', 'datasets/gov/example',
        '-o', remote.url,
        '--credentials', remote.credsfile,
    ])

    resp = remote.app.get('/datasets/gov/example/Country')
    assert listdata(resp, 'code', 'name') == [
        ('ee', 'Estija'),
        ('lt', 'Lietuva'),
        ('lv', 'Latvija')
    ]

    resp = remote.app.get('/datasets/gov/example/City')
    data = listdata(resp, 'country._id', 'name')
    data = [(codes.get(country), city) for country, city in data]
    assert sorted(data) == [
        ('ee', 'Talinas'),
        ('lt', 'Kaunas'),
        ('lt', 'Vilnius'),
        ('lv', 'Ryga'),
    ]


def test_no_primary_key(rc, tmpdir, geodb):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    d | r | b | m | property | source      | type   | ref | access
    datasets/gov/example     |             |        |     |
      | data                 |             | sql    |     |
      |   |                  |             |        |     |
      |   |   | country      | salis       |        |     |
      |   |   |   | code     | kodas       | string |     | open
      |   |   |   | name     | pavadinimas | string |     | open
    '''))

    app = create_client(rc, tmpdir, geodb)

    resp = app.get('/datasets/gov/example/country')
    codes = dict(listdata(resp, '_id', 'code'))
    data = listdata(resp, '_id', 'code', 'name', sort='code')
    data = [(codes.get(_id), code, name) for _id, code, name in data]
    assert data == [
        ('ee', 'ee', 'Estija'),
        ('lt', 'lt', 'Lietuva'),
        ('lv', 'lv', 'Latvija'),
    ]


def test_count(rc, tmpdir, geodb):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    d | r | b | m | property | source      | type   | ref | access
    datasets/gov/example     |             |        |     |
      | data                 |             | sql    |     |
      |   |                  |             |        |     |
      |   |   | country      | salis       |        |     |
      |   |   |   | code     | kodas       | string |     | open
      |   |   |   | name     | pavadinimas | string |     | open
    '''))

    app = create_client(rc, tmpdir, geodb)

    resp = app.get('/datasets/gov/example/country?select(count())')
    assert listdata(resp) == [3]


def test_push_chunks(
    postgresql,
    rc,
    cli: SpintaCliRunner,
    responses,
    tmpdir,
    geodb,
    request,
):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    d | r | b | m | property | source      | type   | ref     | access
    datasets/gov/example     |             |        |         |
      | data                 |             | sql    |         |
      |   |                  |             |        |         |
      |   |   | country      | salis       |        | code    |
      |   |   |   | code     | kodas       | string |         | open
      |   |   |   | name     | pavadinimas | string |         | open
    '''))

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmpdir, geodb)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmpdir, responses)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    cli.invoke(localrc, [
        'push',
        '-d', 'datasets/gov/example',
        '-o', 'spinta+' + remote.url,
        '--credentials', remote.credsfile,
        '--chunk-size=1',
    ])

    remote.app.authmodel('datasets/gov/example/country', ['getall'])
    resp = remote.app.get('/datasets/gov/example/country')
    assert listdata(resp, 'code', 'name') == [
        ('ee', 'Estija'),
        ('lt', 'Lietuva'),
        ('lv', 'Latvija')
    ]


def test_push_state(postgresql, rc, cli: SpintaCliRunner, responses, tmpdir, geodb, request):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    d | r | b | m | property | source      | type   | ref     | access
    datasets/gov/example     |             |        |         |
      | data                 |             | sql    |         |
      |   |                  |             |        |         |
      |   |   | country      | salis       |        | code    |
      |   |   |   | code     | kodas       | string |         | open
      |   |   |   | name     | pavadinimas | string |         | open
    '''))

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmpdir, geodb)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmpdir, responses)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push one row, save state and stop.
    cli.invoke(localrc, [
        'push',
        '-d', 'datasets/gov/example',
        '-o', remote.url,
        '--credentials', remote.credsfile,
        '--chunk-size', '1k',
        '--stop-time', '1h',
        '--stop-row', '1',
        '--state', tmpdir / 'state.db',
    ])

    remote.app.authmodel('datasets/gov/example/country', ['getall'])
    resp = remote.app.get('/datasets/gov/example/country')
    assert len(listdata(resp)) == 1

    cli.invoke(localrc, [
        'push',
        '-d', 'datasets/gov/example',
        '-o', remote.url,
        '--credentials', remote.credsfile,
        '--stop-row', '1',
        '--state', tmpdir / 'state.db',
    ])

    resp = remote.app.get('/datasets/gov/example/country')
    assert len(listdata(resp)) == 2


def test_prepared_property(rc, tmpdir, geodb):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    d | r | b | m | property  | type   | ref  | source      | prepare | access
    datasets/gov/example      |        |      |             |         |
      | data                  | sql    |      |             |         |
      |   |   | country       |        | code | salis       |         | open
      |   |   |   | code      | string |      | kodas       |         |
      |   |   |   | name      | string |      | pavadinimas |         |
      |   |   |   | continent | string |      |             | 'EU'    |
    '''))

    app = create_client(rc, tmpdir, geodb)

    resp = app.get('/datasets/gov/example/country')
    assert listdata(resp, 'continent', 'code', 'name') == [
        ('EU', 'ee', 'Estija'),
        ('EU', 'lt', 'Lietuva'),
        ('EU', 'lv', 'Latvija'),
    ]


def test_composite_keys(rc, tmpdir, sqlite):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    d | r | m | property     | type   | ref           | source    | prepare                 | access
    datasets/ds              |        |               |           |                         |
      | rs                   | sql    |               |           |                         |
      |   | Country          |        | id            | COUNTRY   |                         | open
      |   |   | id           | array  |               |           | code, continent         |
      |   |   | name         | string |               | NAME      |                         |
      |   |   | code         | string |               | CODE      |                         |
      |   |   | continent    | string |               | CONTINENT |                         |
      |   | City             |        | name, country | CITY      |                         | open
      |   |   | name         | string |               | NAME      |                         |
      |   |   | country_code | string |               | COUNTRY   |                         |
      |   |   | continent    | string |               | CONTINENT |                         |
      |   |   | country      | ref    | Country       |           | country_code, continent |
    '''))

    app = create_client(rc, tmpdir, sqlite)

    sqlite.init({
        'COUNTRY': [
            sa.Column('NAME', sa.Text),
            sa.Column('CODE', sa.Text),
            sa.Column('CONTINENT', sa.Text),
        ],
        'CITY': [
            sa.Column('NAME', sa.Text),
            sa.Column('COUNTRY', sa.Text),
            sa.Column('CONTINENT', sa.Text),
        ],
    })

    sqlite.write('COUNTRY', [
        {'CONTINENT': 'eu', 'CODE': 'lt', 'NAME': 'Lithuania'},
        {'CONTINENT': 'eu', 'CODE': 'lv', 'NAME': 'Latvia'},
        {'CONTINENT': 'eu', 'CODE': 'ee', 'NAME': 'Estonia'},
    ])
    sqlite.write('CITY', [
        {'CONTINENT': 'eu', 'COUNTRY': 'lt', 'NAME': 'Vilnius'},
        {'CONTINENT': 'eu', 'COUNTRY': 'lt', 'NAME': 'Kaunas'},
        {'CONTINENT': 'eu', 'COUNTRY': 'lv', 'NAME': 'Riga'},
        {'CONTINENT': 'eu', 'COUNTRY': 'ee', 'NAME': 'Tallinn'},
    ])

    resp = app.get('/datasets/ds/Country')
    data = listdata(resp, '_id', 'continent', 'code', 'name', sort='name')
    country_key_map = {
        _id: (continent, code)
        for _id, continent, code, name in data
    }
    data = [
        (country_key_map.get(_id), continent, code, name)
        for _id, continent, code, name in data
    ]
    assert data == [
        (('eu', 'ee'), 'eu', 'ee', 'Estonia'),
        (('eu', 'lv'), 'eu', 'lv', 'Latvia'),
        (('eu', 'lt'), 'eu', 'lt', 'Lithuania'),
    ]

    resp = app.get('/datasets/ds/City')
    data = listdata(resp, 'country', 'name', sort='name')
    data = [
        (
            {
                **country,
                '_id': country_key_map.get(country.get('_id')),
            },
            name,
        )
        for country, name in data
    ]
    assert data == [
        ({'_id': ('eu', 'lt')}, 'Kaunas'),
        ({'_id': ('eu', 'lv')}, 'Riga'),
        ({'_id': ('eu', 'ee')}, 'Tallinn'),
        ({'_id': ('eu', 'lt')}, 'Vilnius'),
    ]


def test_composite_non_pk_keys(rc, tmpdir, sqlite):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    d | r | m | property     | type   | ref                     | source    | prepare                 | access
    datasets/ds              |        |                         |           |                         |
      | rs                   | sql    |                         |           |                         |
      |   | Country          |        | code                    | COUNTRY   |                         | open
      |   |   | name         | string |                         | NAME      |                         |
      |   |   | code         | string |                         | CODE      |                         |
      |   |   | continent    | string |                         | CONTINENT |                         |
      |   | City             |        | name, country           | CITY      |                         | open
      |   |   | name         | string |                         | NAME      |                         |
      |   |   | country_code | string |                         | COUNTRY   |                         |
      |   |   | continent    | string |                         | CONTINENT |                         |
      |   |   | country      | ref    | Country[continent,code] |           | continent, country_code |
    '''))

    app = create_client(rc, tmpdir, sqlite)

    sqlite.init({
        'COUNTRY': [
            sa.Column('NAME', sa.Text),
            sa.Column('CODE', sa.Text),
            sa.Column('CONTINENT', sa.Text),
        ],
        'CITY': [
            sa.Column('NAME', sa.Text),
            sa.Column('COUNTRY', sa.Text),
            sa.Column('CONTINENT', sa.Text),
        ],
    })

    sqlite.write('COUNTRY', [
        {'CONTINENT': 'eu', 'CODE': 'lt', 'NAME': 'Lithuania'},
        {'CONTINENT': 'eu', 'CODE': 'lv', 'NAME': 'Latvia'},
        {'CONTINENT': 'eu', 'CODE': 'ee', 'NAME': 'Estonia'},
    ])
    sqlite.write('CITY', [
        {'CONTINENT': 'eu', 'COUNTRY': 'lt', 'NAME': 'Vilnius'},
        {'CONTINENT': 'eu', 'COUNTRY': 'lt', 'NAME': 'Kaunas'},
        {'CONTINENT': 'eu', 'COUNTRY': 'lv', 'NAME': 'Riga'},
        {'CONTINENT': 'eu', 'COUNTRY': 'ee', 'NAME': 'Tallinn'},
    ])

    resp = app.get('/datasets/ds/Country')
    data = listdata(resp, '_id', 'continent', 'code', 'name', sort='name')
    country_key_map = {
        _id: (continent, code)
        for _id, continent, code, name in data
    }
    data = [
        (country_key_map.get(_id), continent, code, name)
        for _id, continent, code, name in data
    ]
    assert data == [
        (('eu', 'ee'), 'eu', 'ee', 'Estonia'),
        (('eu', 'lv'), 'eu', 'lv', 'Latvia'),
        (('eu', 'lt'), 'eu', 'lt', 'Lithuania'),
    ]

    resp = app.get('/datasets/ds/City')
    data = listdata(resp, 'country', 'name', sort='name')
    data = [
        (
            {
                **country,
                '_id': country_key_map.get(country.get('_id')),
            },
            name,
        )
        for country, name in data
    ]
    assert data == [
        ({'_id': ('eu', 'lt')}, 'Kaunas'),
        ({'_id': ('eu', 'lv')}, 'Riga'),
        ({'_id': ('eu', 'ee')}, 'Tallinn'),
        ({'_id': ('eu', 'lt')}, 'Vilnius'),
    ]


def test_composite_non_pk_keys_with_filter(rc, tmpdir, sqlite):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    d | r | m | property     | type   | ref                     | source    | prepare                 | access
    datasets/ds              |        |                         |           |                         |
      | rs                   | sql    |                         |           |                         |
      |   | Country          |        | code                    | COUNTRY   |                         | open
      |   |   | name         | string |                         | NAME      |                         |
      |   |   | code         | string |                         | CODE      |                         |
      |   |   | continent    | string |                         | CONTINENT |                         |
      |   | City             |        | name, country           | CITY      | country.code='lt'       | open
      |   |   | name         | string |                         | NAME      |                         |
      |   |   | country_code | string |                         | COUNTRY   |                         |
      |   |   | continent    | string |                         | CONTINENT |                         |
      |   |   | country      | ref    | Country[continent,code] |           | continent, country_code |
    '''))

    app = create_client(rc, tmpdir, sqlite)

    sqlite.init({
        'COUNTRY': [
            sa.Column('NAME', sa.Text),
            sa.Column('CODE', sa.Text),
            sa.Column('CONTINENT', sa.Text),
        ],
        'CITY': [
            sa.Column('NAME', sa.Text),
            sa.Column('COUNTRY', sa.Text),
            sa.Column('CONTINENT', sa.Text),
        ],
    })

    sqlite.write('COUNTRY', [
        {'CONTINENT': 'eu', 'CODE': 'lt', 'NAME': 'Lithuania'},
        {'CONTINENT': 'eu', 'CODE': 'lv', 'NAME': 'Latvia'},
        {'CONTINENT': 'eu', 'CODE': 'ee', 'NAME': 'Estonia'},
    ])
    sqlite.write('CITY', [
        {'CONTINENT': 'eu', 'COUNTRY': 'lt', 'NAME': 'Vilnius'},
        {'CONTINENT': 'eu', 'COUNTRY': 'lt', 'NAME': 'Kaunas'},
        {'CONTINENT': 'eu', 'COUNTRY': 'lv', 'NAME': 'Riga'},
        {'CONTINENT': 'eu', 'COUNTRY': 'ee', 'NAME': 'Tallinn'},
    ])

    resp = app.get('/datasets/ds/Country')
    data = listdata(resp, '_id', 'continent', 'code', 'name', sort='name')
    country_key_map = {
        _id: (continent, code)
        for _id, continent, code, name in data
    }
    data = [
        (country_key_map.get(_id), continent, code, name)
        for _id, continent, code, name in data
    ]
    assert data == [
        (('eu', 'ee'), 'eu', 'ee', 'Estonia'),
        (('eu', 'lv'), 'eu', 'lv', 'Latvia'),
        (('eu', 'lt'), 'eu', 'lt', 'Lithuania'),
    ]

    resp = app.get('/datasets/ds/City')
    data = listdata(resp, 'country', 'name', sort='name')
    data = [
        (
            {
                **country,
                '_id': country_key_map.get(country.get('_id')),
            },
            name,
        )
        for country, name in data
    ]
    assert data == [
        ({'_id': ('eu', 'lt')}, 'Kaunas'),
        ({'_id': ('eu', 'lt')}, 'Vilnius'),
    ]


def test_access_private_primary_key(rc, tmpdir, sqlite):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    d | r | m | property     | type   | ref     | source  | access
    datasets/ds              |        |         |         |
      | rs                   | sql    |         |         |
      |   | Country          |        | code    | COUNTRY |     
      |   |   | name         | string |         | NAME    | open
      |   |   | code         | string |         | CODE    | private
      |   | City             |        | country | CITY    |     
      |   |   | name         | string |         | NAME    | open
      |   |   | country      | ref    | Country | COUNTRY | open
    '''))

    app = create_client(rc, tmpdir, sqlite)

    sqlite.init({
        'COUNTRY': [
            sa.Column('NAME', sa.Text),
            sa.Column('CODE', sa.Text),
        ],
        'CITY': [
            sa.Column('NAME', sa.Text),
            sa.Column('COUNTRY', sa.Text),
        ],
    })

    sqlite.write('COUNTRY', [
        {'CODE': 'lt', 'NAME': 'Lithuania'},
        {'CODE': 'lv', 'NAME': 'Latvia'},
        {'CODE': 'ee', 'NAME': 'Estonia'},
    ])
    sqlite.write('CITY', [
        {'COUNTRY': 'lt', 'NAME': 'Vilnius'},
        {'COUNTRY': 'lt', 'NAME': 'Kaunas'},
        {'COUNTRY': 'lv', 'NAME': 'Riga'},
        {'COUNTRY': 'ee', 'NAME': 'Tallinn'},
    ])

    resp = app.get('/datasets/ds/Country')
    data = listdata(resp, '_id', 'code', 'name', sort='name')
    country_key_map = {_id: name for _id, code, name in data}
    data = [
        (country_key_map.get(_id), code, name)
        for _id, code, name in data
    ]
    assert data == [
        ('Estonia', NA, 'Estonia'),
        ('Latvia', NA, 'Latvia'),
        ('Lithuania', NA, 'Lithuania'),
    ]

    resp = app.get('/datasets/ds/City')
    data = listdata(resp, 'country', 'name', sort='name')
    data = [
        (
            {
                **country,
                '_id': country_key_map.get(country.get('_id')),
            },
            name,
        )
        for country, name in data
    ]
    assert data == [
        ({'_id': 'Lithuania'}, 'Kaunas'),
        ({'_id': 'Latvia'}, 'Riga'),
        ({'_id': 'Estonia'}, 'Tallinn'),
        ({'_id': 'Lithuania'}, 'Vilnius'),
    ]


def test_enum(rc, tmpdir, sqlite):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    d | r | m | property     | type   | ref     | source  | prepare | access
    datasets/gov/example     |        |         |         |         |
      | resource             | sql    |         |         |         |
      |   | Country          |        | name    | COUNTRY |         |     
      |   |   | name         | string |         | NAME    |         | open
      |   |   | driving      | string |         | DRIVING |         | open
                             | enum   |         | l       | 'left'  | open
                             |        |         | r       | 'right' | open
    '''))

    sqlite.init({
        'COUNTRY': [
            sa.Column('NAME', sa.Text),
            sa.Column('DRIVING', sa.Text),
        ],
    })

    sqlite.write('COUNTRY', [
        {'DRIVING': 'r', 'NAME': 'Lithuania'},
        {'DRIVING': 'r', 'NAME': 'Latvia'},
        {'DRIVING': 'l', 'NAME': 'India'},
    ])

    app = create_client(rc, tmpdir, sqlite)
    resp = app.get('/datasets/gov/example/Country')
    assert listdata(resp) == [
        ('left', 'India'),
        ('right', 'Latvia'),
        ('right', 'Lithuania'),
    ]


def test_enum_no_prepare(rc, tmpdir, sqlite):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    d | r | m | property     | type   | ref     | source  | prepare | access
    datasets/gov/example     |        |         |         |         |
      | resource             | sql    |         |         |         |
      |   | Country          |        | name    | COUNTRY |         |     
      |   |   | name         | string |         | NAME    |         | open
      |   |   | driving      | string |         | DRIVING |         | open
                             | enum   |         | l       |         | open
                             |        |         | r       |         | open
    '''))

    sqlite.init({
        'COUNTRY': [
            sa.Column('NAME', sa.Text),
            sa.Column('DRIVING', sa.Text),
        ],
    })

    sqlite.write('COUNTRY', [
        {'DRIVING': 'r', 'NAME': 'Lithuania'},
        {'DRIVING': 'r', 'NAME': 'Latvia'},
        {'DRIVING': 'l', 'NAME': 'India'},
    ])

    app = create_client(rc, tmpdir, sqlite)
    resp = app.get('/datasets/gov/example/Country')
    assert listdata(resp) == [
        ('l', 'India'),
        ('r', 'Latvia'),
        ('r', 'Lithuania'),
    ]


def test_enum_empty_source(rc, tmpdir, sqlite):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    d | r | m | property     | type   | ref     | source  | prepare       | access
    datasets/gov/example     |        |         |         |               |
      | resource             | sql    |         |         |               |
      |   | Country          |        | name    | COUNTRY |               |     
      |   |   | name         | string |         | NAME    |               | open
      |   |   | driving      | string |         | DRIVING | swap('', '-') | open
                             | enum   |         | l       |               | open
                             |        |         | r       |               | open
                             |        |         | -       | null          | open
    '''))

    sqlite.init({
        'COUNTRY': [
            sa.Column('NAME', sa.Text),
            sa.Column('DRIVING', sa.Text),
        ],
    })

    sqlite.write('COUNTRY', [
        {'DRIVING': 'r', 'NAME': 'Lithuania'},
        {'DRIVING': 'r', 'NAME': 'Latvia'},
        {'DRIVING': '', 'NAME': 'Antarctica'},
    ])

    app = create_client(rc, tmpdir, sqlite)
    resp = app.get('/datasets/gov/example/Country')
    assert listdata(resp) == [
        ('r', 'Latvia'),
        ('r', 'Lithuania'),
        (None, 'Antarctica'),
    ]


def test_enum_empty_integer_source(rc, tmpdir, sqlite):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    d | r | m | property     | type   | ref     | source  | prepare | access
    datasets/gov/example     |        |         |         |         |
      | resource             | sql    |         |         |         |
      |   | Country          |        | name    | COUNTRY |         |     
      |   |   | name         | string |         | NAME    |         | open
      |   |   | driving      | string |         | DRIVING |         | open
                             | enum   |         | 0       | 'l'     | open
                             |        |         | 1       | 'r'     | open
    '''))

    sqlite.init({
        'COUNTRY': [
            sa.Column('NAME', sa.Text),
            sa.Column('DRIVING', sa.Integer),
        ],
    })

    sqlite.write('COUNTRY', [
        {'DRIVING': 0, 'NAME': 'India'},
        {'DRIVING': 1, 'NAME': 'Lithuania'},
        {'DRIVING': 1, 'NAME': 'Latvia'},
    ])

    app = create_client(rc, tmpdir, sqlite)
    resp = app.get('/datasets/gov/example/Country')
    assert listdata(resp) == [
        ('l', 'India'),
        ('r', 'Latvia'),
        ('r', 'Lithuania'),
    ]


def test_filter_by_enum_access(rc, tmpdir, sqlite):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    d | r | m | property     | type   | ref     | source  | prepare | access
    datasets/gov/example     |        |         |         |         |
      | resource             | sql    |         |         |         |
      |   | Country          |        | name    | COUNTRY |         |     
      |   |   | name         | string |         | NAME    |         | open
      |   |   | driving      | string |         | DRIVING |         | open
                             | enum   |         | 0       | 'l'     | private
                             |        |         | 1       | 'r'     | open
    '''))

    sqlite.init({
        'COUNTRY': [
            sa.Column('NAME', sa.Text),
            sa.Column('DRIVING', sa.Integer),
        ],
    })

    sqlite.write('COUNTRY', [
        {'DRIVING': 0, 'NAME': 'India'},
        {'DRIVING': 1, 'NAME': 'Lithuania'},
        {'DRIVING': 1, 'NAME': 'Latvia'},
    ])

    app = create_client(rc, tmpdir, sqlite)
    resp = app.get('/datasets/gov/example/Country')
    assert listdata(resp) == [
        ('r', 'Latvia'),
        ('r', 'Lithuania'),
    ]


def test_filter_by_enum(rc, tmpdir, sqlite):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    d | r | m | property     | type   | ref     | source  | prepare | access
    datasets/gov/example     |        |         |         |         |
      | resource             | sql    |         |         |         |
      |   | Country          |        | name    | COUNTRY |         |     
      |   |   | name         | string |         | NAME    |         | open
      |   |   | driving      | string |         | DRIVING |         | open
                             | enum   |         | 0       | 'l'     | private
                             |        |         | 1       | 'r'     | open
    '''))

    sqlite.init({
        'COUNTRY': [
            sa.Column('NAME', sa.Text),
            sa.Column('DRIVING', sa.Integer),
        ],
    })

    sqlite.write('COUNTRY', [
        {'DRIVING': 0, 'NAME': 'India'},
        {'DRIVING': 1, 'NAME': 'Lithuania'},
        {'DRIVING': 1, 'NAME': 'Latvia'},
    ])

    app = create_client(rc, tmpdir, sqlite)
    resp = app.get('/datasets/gov/example/Country?driving="r"')
    assert listdata(resp) == [
        ('r', 'Latvia'),
        ('r', 'Lithuania'),
    ]


def test_filter_by_enum_multi_value(rc, tmpdir, sqlite):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    d | r | m | property     | type   | ref     | source  | prepare | access
    datasets/gov/example     |        |         |         |         |
      | resource             | sql    |         |         |         |
      |   | Country          |        | name    | COUNTRY |         |     
      |   |   | name         | string |         | NAME    |         | open
      |   |   | driving      | string |         | DRIVING |         | open
                             | enum   |         | 0       | 'l'     |
                             |        |         | 1       | 'r'     |
                             |        |         | 2       | 'r'     |
    '''))

    sqlite.init({
        'COUNTRY': [
            sa.Column('NAME', sa.Text),
            sa.Column('DRIVING', sa.Integer),
        ],
    })

    sqlite.write('COUNTRY', [
        {'DRIVING': 0, 'NAME': 'India'},
        {'DRIVING': 1, 'NAME': 'Lithuania'},
        {'DRIVING': 2, 'NAME': 'Latvia'},
    ])

    app = create_client(rc, tmpdir, sqlite)
    resp = app.get('/datasets/gov/example/Country?driving="r"')
    assert listdata(resp) == [
        ('r', 'Latvia'),
        ('r', 'Lithuania'),
    ]


def test_filter_by_enum_list_value(rc, tmpdir, sqlite):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    d | r | m | property     | type   | ref     | source  | prepare | access
    datasets/gov/example     |        |         |         |         |
      | resource             | sql    |         |         |         |
      |   | Country          |        | name    | COUNTRY |         |     
      |   |   | name         | string |         | NAME    |         | open
      |   |   | driving      | string |         | DRIVING |         | open
                             | enum   |         | 0       | 'l'     |
                             |        |         | 1       | 'r'     |
    '''))

    sqlite.init({
        'COUNTRY': [
            sa.Column('NAME', sa.Text),
            sa.Column('DRIVING', sa.Integer),
        ],
    })

    sqlite.write('COUNTRY', [
        {'DRIVING': 0, 'NAME': 'India'},
        {'DRIVING': 1, 'NAME': 'Lithuania'},
        {'DRIVING': 1, 'NAME': 'Latvia'},
    ])

    app = create_client(rc, tmpdir, sqlite)
    resp = app.get('/datasets/gov/example/Country?driving=["l","r"]')
    assert listdata(resp) == [
        ('l', 'India'),
        ('r', 'Latvia'),
        ('r', 'Lithuania'),
    ]


def test_implicit_filter(rc, tmpdir, sqlite):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    d | r | m | property     | type   | ref     | source    | prepare          | access
    datasets/gov/example     |        |         |           |                  |
      | resource             | sql    |         |           |                  |
      |   | Country          |        | code    | COUNTRY   | continent = 'eu' |     
      |   |   | code         | string |         | CODE      |                  | open
      |   |   | continent    | string |         | CONTINENT |                  | open
      |   |   | name         | string |         | NAME      |                  | open
                             |        |         |           |                  |
      |   | City             |        | name    | CITY      |                  |     
      |   |   | name         | string |         | NAME      |                  | open
      |   |   | country      | ref    | Country | COUNTRY   |                  | open
    '''))

    sqlite.init({
        'COUNTRY': [
            sa.Column('CODE', sa.String),
            sa.Column('CONTINENT', sa.String),
            sa.Column('NAME', sa.Text),
        ],
        'CITY': [
            sa.Column('NAME', sa.Text),
            sa.Column('COUNTRY', sa.String),
        ],
    })

    sqlite.write('COUNTRY', [
        {'CODE': 'in', 'CONTINENT': 'as', 'NAME': 'India'},
        {'CODE': 'lt', 'CONTINENT': 'eu', 'NAME': 'Lithuania'},
        {'CODE': 'lv', 'CONTINENT': 'eu', 'NAME': 'Latvia'},
    ])

    sqlite.write('CITY', [
        {'COUNTRY': 'in', 'NAME': 'Mumbai'},
        {'COUNTRY': 'in', 'NAME': 'Delhi'},
        {'COUNTRY': 'lt', 'NAME': 'Vilnius'},
        {'COUNTRY': 'lv', 'NAME': 'Ryga'},
    ])

    app = create_client(rc, tmpdir, sqlite)
    resp = app.get('/datasets/gov/example/City')
    assert listdata(resp, 'name') == [
        'Ryga',
        'Vilnius',
    ]


def test_implicit_filter_by_enum(rc, tmpdir, sqlite):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    d | r | m | property     | type   | ref     | source  | prepare | access
    datasets/gov/example     |        |         |         |         |
      | resource             | sql    |         |         |         |
      |   | Country          |        | id      | COUNTRY |         |     
      |   |   | id           | string |         | ID      |         | private
      |   |   | name         | string |         | NAME    |         | open
      |   |   | driving      | string |         | DRIVING |         | open
                             | enum   |         | 0       | 'l'     | protected
                             |        |         | 1       | 'r'     |
                             |        |         |         |         |
      |   | City             |        | name    | CITY    |         |     
      |   |   | name         | string |         | NAME    |         | open
      |   |   | country      | ref    | Country | COUNTRY |         | open
    '''))

    sqlite.init({
        'COUNTRY': [
            sa.Column('ID', sa.Integer),
            sa.Column('NAME', sa.Text),
            sa.Column('DRIVING', sa.Integer),
        ],
        'CITY': [
            sa.Column('NAME', sa.Text),
            sa.Column('COUNTRY', sa.Integer),
        ],
    })

    sqlite.write('COUNTRY', [
        {'ID': 1, 'DRIVING': 0, 'NAME': 'India'},
        {'ID': 2, 'DRIVING': 1, 'NAME': 'Lithuania'},
        {'ID': 3, 'DRIVING': 1, 'NAME': 'Latvia'},
    ])

    sqlite.write('CITY', [
        {'COUNTRY': 1, 'NAME': 'Mumbai'},
        {'COUNTRY': 1, 'NAME': 'Delhi'},
        {'COUNTRY': 2, 'NAME': 'Vilnius'},
        {'COUNTRY': 3, 'NAME': 'Ryga'},
    ])

    app = create_client(rc, tmpdir, sqlite)
    resp = app.get('/datasets/gov/example/City')
    assert listdata(resp, 'name') == [
        'Ryga',
        'Vilnius',
    ]


def test_file(rc, tmpdir, sqlite):
    create_tabular_manifest(tmpdir / 'manifest.csv', '''
    d | r | m | property  | type   | ref | source    | prepare                                   | access
    datasets/gov/example  |        |     |           |                                           |
      | resource          | sql    |     |           |                                           |
      |   | Country       |        | id  | COUNTRY   |                                           |     
      |   |   | id        | string |     | ID        |                                           | private
      |   |   | name      | string |     | NAME      |                                           | open
      |   |   | flag_name | string |     | FLAG_FILE |                                           | private
      |   |   | flag_data | binary |     | FLAG_DATA |                                           | private
      |   |   | flag      | file   |     |           | file(name: flag_name, content: flag_data) | open
    ''')

    sqlite.init({
        'COUNTRY': [
            sa.Column('ID', sa.Integer),
            sa.Column('NAME', sa.Text),
            sa.Column('FLAG_FILE', sa.Text),
            sa.Column('FLAG_DATA', sa.LargeBinary),
        ],
    })

    sqlite.write('COUNTRY', [
        {
            'ID': 2,
            'NAME': 'Lithuania',
            'FLAG_FILE': 'lt.png',
            'FLAG_DATA': b'DATA',
        },
    ])

    app = create_client(rc, tmpdir, sqlite)
    resp = app.get('/datasets/gov/example/Country')
    assert listdata(resp, full=True) == [
        {
            'name': 'Lithuania',
            'flag._id': 'lt.png',
            'flag._content_type': None,  # FIXME: Should be 'image/png'.
        },
    ]


def test_push_file(
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmpdir,
    sqlite: Sqlite,
    request,
):
    create_tabular_manifest(tmpdir / 'manifest.csv', '''
    d | r | m | property   | type   | ref | source    | prepare                                   | access
    datasets/gov/push/file |        |     |           |                                           |
      | resource           | sql    | sql |           |                                           |
      |   | Country        |        | id  | COUNTRY   |                                           |     
      |   |   | id         | string |     | ID        |                                           | private
      |   |   | name       | string |     | NAME      |                                           | open
      |   |   | flag_name  | string |     | FLAG_FILE |                                           | private
      |   |   | flag_data  | binary |     | FLAG_DATA |                                           | private
      |   |   | flag       | file   |     |           | file(name: flag_name, content: flag_data) | open
    ''')

    # Configure local server with SQL backend
    sqlite.init({
        'COUNTRY': [
            sa.Column('ID', sa.Integer),
            sa.Column('NAME', sa.Text),
            sa.Column('FLAG_FILE', sa.Text),
            sa.Column('FLAG_DATA', sa.LargeBinary),
        ],
    })
    sqlite.write('COUNTRY', [
        {
            'ID': 2,
            'NAME': 'Lithuania',
            'FLAG_FILE': 'lt.png',
            'FLAG_DATA': b'DATA',
        },
    ])
    local_rc = create_rc(rc, tmpdir, sqlite)

    # Configure remote server
    remote = configure_remote_server(cli, local_rc, rc, tmpdir, responses)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data to the remote server
    cli.invoke(local_rc, [
        'push',
        str(tmpdir / 'manifest.csv'),
        '-o', remote.url,
        '--credentials', remote.credsfile,
    ])

    remote.app.authmodel('datasets/gov/push/file/Country', ['getall', 'getone'])
    resp = remote.app.get('/datasets/gov/push/file/Country')
    assert listdata(resp, full=True) == [
        {
            'name': 'Lithuania',
            'flag._id': 'lt.png',
            'flag._content_type': None,  # FIXME: Should be 'image/png'.
        },
    ]
    _id = resp.json()['_data'][0]['_id']
    resp = remote.app.get(f'/datasets/gov/push/file/Country/{_id}/flag')
    assert resp.status_code == 200
    assert resp.content == b'DATA'


def test_image(rc, tmpdir, sqlite):
    create_tabular_manifest(tmpdir / 'manifest.csv', '''
    d | r | m | property  | type   | ref | source    | prepare                                   | access
    datasets/gov/example  |        |     |           |                                           |
      | resource          | sql    |     |           |                                           |
      |   | Country       |        | id  | COUNTRY   |                                           |     
      |   |   | id        | string |     | ID        |                                           | private
      |   |   | name      | string |     | NAME      |                                           | open
      |   |   | flag_name | string |     | FLAG_FILE |                                           | private
      |   |   | flag_data | binary |     | FLAG_DATA |                                           | private
      |   |   | flag      | image  |     |           | file(name: flag_name, content: flag_data) | open
    ''')

    sqlite.init({
        'COUNTRY': [
            sa.Column('ID', sa.Integer),
            sa.Column('NAME', sa.Text),
            sa.Column('FLAG_FILE', sa.Text),
            sa.Column('FLAG_DATA', sa.LargeBinary),
        ],
    })

    sqlite.write('COUNTRY', [
        {
            'ID': 2,
            'NAME': 'Lithuania',
            'FLAG_FILE': 'lt.png',
            'FLAG_DATA': b'DATA',
        },
    ])

    app = create_client(rc, tmpdir, sqlite)
    resp = app.get('/datasets/gov/example/Country')
    assert listdata(resp, full=True) == [
        {
            'name': 'Lithuania',
            'flag._id': 'lt.png',
            'flag._content_type': None,  # FIXME: Should be 'image/png'.
        },
    ]


def test_image_file(
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmpdir,
    sqlite: Sqlite,
    request,
):
    create_tabular_manifest(tmpdir / 'manifest.csv', '''
    d | r | m | property   | type   | ref | source    | prepare                                   | access
    datasets/gov/push/file |        |     |           |                                           |
      | resource           | sql    | sql |           |                                           |
      |   | Country        |        | id  | COUNTRY   |                                           |     
      |   |   | id         | string |     | ID        |                                           | private
      |   |   | name       | string |     | NAME      |                                           | open
      |   |   | flag_name  | string |     | FLAG_FILE |                                           | private
      |   |   | flag_data  | binary |     | FLAG_DATA |                                           | private
      |   |   | flag       | image  |     |           | file(name: flag_name, content: flag_data) | open
    ''')

    # Configure local server with SQL backend
    sqlite.init({
        'COUNTRY': [
            sa.Column('ID', sa.Integer),
            sa.Column('NAME', sa.Text),
            sa.Column('FLAG_FILE', sa.Text),
            sa.Column('FLAG_DATA', sa.LargeBinary),
        ],
    })
    sqlite.write('COUNTRY', [
        {
            'ID': 2,
            'NAME': 'Lithuania',
            'FLAG_FILE': 'lt.png',
            'FLAG_DATA': b'DATA',
        },
    ])
    local_rc = create_rc(rc, tmpdir, sqlite)

    # Configure remote server
    remote = configure_remote_server(cli, local_rc, rc, tmpdir, responses)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data to the remote server
    cli.invoke(local_rc, [
        'push',
        str(tmpdir / 'manifest.csv'),
        '-o', remote.url,
        '--credentials', remote.credsfile,
    ])

    remote.app.authmodel('datasets/gov/push/file/Country', ['getall', 'getone'])
    resp = remote.app.get('/datasets/gov/push/file/Country')
    assert listdata(resp, full=True) == [
        {
            'name': 'Lithuania',
            'flag._id': 'lt.png',
            'flag._content_type': None,  # FIXME: Should be 'image/png'.
        },
    ]
    _id = resp.json()['_data'][0]['_id']
    resp = remote.app.get(f'/datasets/gov/push/file/Country/{_id}/flag')
    assert resp.status_code == 200
    assert resp.content == b'DATA'


def test_push_null_foreign_key(
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmpdir,
    sqlite: Sqlite,
    request,
):
    create_tabular_manifest(tmpdir / 'manifest.csv', '''
    d | r | b | m | property      | type     | ref          | source        | access
    example/null/fk               |          |              |               |
      | resource                  | sql      | sql          |               |
      |   |   | Country           |          | id           | COUNTRY       |
      |   |   |   | id            | integer  |              | ID            | private
      |   |   |   | name          | string   |              | NAME          | open
      |   |   | City              |          | id           | CITY          |
      |   |   |   | id            | integer  |              | ID            | private
      |   |   |   | name          | string   |              | NAME          | open
      |   |   |   | country       | ref      | Country      | COUNTRY       | open
      |   |   |   | embassy       | ref      | Country      | EMBASSY       | open
    ''')

    # Configure local server with SQL backend
    sqlite.init({
        'COUNTRY': [
            sa.Column('ID',           sa.Integer),
            sa.Column('NAME',         sa.String),
        ],
        'CITY': [
            sa.Column('ID',           sa.Integer),
            sa.Column('NAME',         sa.String),
            sa.Column('COUNTRY',      sa.Integer, sa.ForeignKey('COUNTRY.ID')),
            sa.Column('EMBASSY',      sa.Integer, sa.ForeignKey('COUNTRY.ID')),
        ],
    })
    sqlite.write('COUNTRY', [
        {
            'ID': 1,
            'NAME': 'Latvia',
        },
        {
            'ID': 2,
            'NAME': 'Lithuania',
        },
    ])
    sqlite.write('CITY', [
        {
            'ID': 1,
            'NAME': 'Ryga',
            'COUNTRY': 1,
            'EMBASSY': None,
        },
        {
            'ID': 2,
            'NAME': 'Vilnius',
            'COUNTRY': 2,
            'EMBASSY': 1,
        },
        {
            'ID': 3,
            'NAME': 'Winterfell',
            'COUNTRY': None,
            'EMBASSY': None,
        },
    ])
    local_rc = create_rc(rc, tmpdir, sqlite)

    # Configure remote server
    remote = configure_remote_server(cli, local_rc, rc, tmpdir, responses)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    cli.invoke(local_rc, [
        'push',
        '-o', 'spinta+' + remote.url,
        '--credentials', remote.credsfile,
    ])

    remote.app.authmodel('example/null/fk', ['getall'])

    resp = remote.app.get('/example/null/fk/Country')
    countries = dict(listdata(resp, 'name', '_id'))

    resp = remote.app.get('/example/null/fk/City')
    assert listdata(resp, full=True) == [
        {
            'name': 'Ryga',
            'country._id': countries['Latvia'],
            'embassy._id': None,
        },
        {
            'name': 'Vilnius',
            'country._id': countries['Lithuania'],
            'embassy._id': countries['Latvia'],
        },
        {
            'name': 'Winterfell',
            'country._id': None,
            'embassy._id': None,
        },
    ]


def test_push_self_ref(
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmpdir,
    sqlite: Sqlite,
    request,
):
    create_tabular_manifest(tmpdir / 'manifest.csv', '''
    d | r | b | m | property      | type     | ref          | source        | access
    example/self/ref              |          |              |               |
      | resource                  | sql      | sql          |               |
      |   |   | City              |          | id           | CITY          |
      |   |   |   | id            | integer  |              | ID            | private
      |   |   |   | name          | string   |              | NAME          | open
      |   |   |   | governance    | ref      | City         | GOVERNANCE    | open
    ''')

    # Configure local server with SQL backend
    sqlite.init({
        'CITY': [
            sa.Column('ID',           sa.Integer),
            sa.Column('NAME',         sa.String),
            sa.Column('GOVERNANCE',   sa.Integer, sa.ForeignKey('CITY.ID')),
        ],
    })
    sqlite.write('CITY', [
        {
            'ID': 1,
            'NAME': 'Vilnius',
            'GOVERNANCE': None,
        },
        {
            'ID': 2,
            'NAME': 'Trakai',
            'GOVERNANCE': 1,
        },
    ])
    local_rc = create_rc(rc, tmpdir, sqlite)

    # Configure remote server
    remote = configure_remote_server(cli, local_rc, rc, tmpdir, responses)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    cli.invoke(local_rc, [
        'push',
        '-o', 'spinta+' + remote.url,
        '--credentials', remote.credsfile,
    ])

    remote.app.authmodel('example/self/ref', ['getall'])

    resp = remote.app.get('/example/self/ref/City')
    cities = dict(listdata(resp, 'name', '_id'))
    assert listdata(resp, full=True) == [
        {
            'name': 'Trakai',
            'governance._id': cities['Vilnius'],
        },
        {
            'name': 'Vilnius',
            'governance._id': None,
        },
    ]
