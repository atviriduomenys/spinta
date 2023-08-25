import logging
import pathlib
import re
from typing import Dict
from typing import Tuple

import pytest

import sqlalchemy as sa
from _pytest.logging import LogCaptureFixture
from requests import PreparedRequest
from responses import POST
from responses import RequestsMock

from spinta.client import add_client_credentials
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


def create_rc(rc: RawConfig, tmp_path: pathlib.Path, db: Sqlite) -> RawConfig:
    return rc.fork({
        'manifests': {
            'default': {
                'type': 'tabular',
                'path': str(tmp_path / 'manifest.csv'),
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
    tmp_path: pathlib.Path,
    responses,
    remove_source: bool = True
):
    invoke_props = [
        'copy',
        '--access', 'open',
        '-o', tmp_path / 'remote.csv',
        tmp_path / 'manifest.csv',
    ]
    if remove_source:
        invoke_props.append('--no-source')
    cli.invoke(local_rc, invoke_props)

    # Create remote server with PostgreSQL backend
    remote_rc = rc.fork({
        'manifests': {
            'default': {
                'type': 'tabular',
                'path': str(tmp_path / 'remote.csv'),
                'backend': 'default',
            },
        },
        'backends': ['default'],
    })
    return create_remote_server(
        remote_rc,
        tmp_path,
        responses,
        scopes=[
            'spinta_set_meta_fields',
            'spinta_getone',
            'spinta_getall',
            'spinta_search',
            'spinta_insert',
            'spinta_patch',
            'spinta_delete',
        ],
        credsfile=True,
    )


def create_client(rc: RawConfig, tmp_path: pathlib.Path, geodb: Sqlite):
    rc = create_rc(rc, tmp_path, geodb)
    context = create_test_context(rc)
    return create_test_client(context)


def test_filter(rc, tmp_path, geodb):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
    id | d | r | b | m | property | source      | prepare   | type   | ref     | level | access | uri | title   | description
       | datasets/gov/example     |             |           |        |         |       |        |     | Example |
       |   | data                 |             |           | sql    |         |       |        |     | Data    |
       |   |   |                  |             |           |        |         |       |        |     |         |
       |   |   |   | country      | salis       | code='lt' |        | code    |       |        |     | Country |
       |   |   |   |   | code     | kodas       |           | string |         | 3     | open   |     | Code    |
       |   |   |   |   | name     | pavadinimas |           | string |         | 3     | open   |     | Name    |
    '''))

    app = create_client(rc, tmp_path, geodb)

    resp = app.get('/datasets/gov/example/country')
    assert listdata(resp, 'code', 'name') == [
        ('lt', 'Lietuva'),
    ]


def test_filter_join(rc, tmp_path, geodb):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
    id | d | r | b | m | property | type   | ref     | source      | prepare           | level | access | uri | title   | description
       | datasets/gov/example     |        |         |             |                   |       |        |     | Example |
       |   | data                 | sql    |         |             |                   |       |        |     | Data    |
       |   |   |                  |        |         |             |                   |       |        |     |         |
       |   |   |   | Country      |        | code    | salis       |                   |       |        |     | Country |
       |   |   |   |   | code     | string |         | kodas       |                   | 3     | open   |     | Code    |
       |   |   |   |   | name     | string |         | pavadinimas |                   | 3     | open   |     | Name    |
       |   |   |                  |        |         |             |                   |       |        |     |         |
       |   |   |   | City         |        | name    | miestas     | country.code='lt' |       |        |     | City    |
       |   |   |   |   | name     | string |         | pavadinimas |                   | 3     | open   |     | Name    |
       |   |   |   |   | country  | ref    | Country | salis       |                   | 4     | open   |     | Country |
    '''))

    app = create_client(rc, tmp_path, geodb)

    resp = app.get('/datasets/gov/example/Country')
    codes = dict(listdata(resp, '_id', 'code'))

    resp = app.get('/datasets/gov/example/City?sort(name)')
    data = listdata(resp, 'country._id', 'name')
    data = [(codes.get(country), city) for country, city in data]
    assert data == [
        ('lt', 'Vilnius'),
    ]


def test_filter_join_nested(
    rc: RawConfig,
    tmp_path: pathlib.Path,
    sqlite: Sqlite,
):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property | type   | ref     | source   | prepare   | access
    example/join/nested      |        |         |          |           |
      | data                 | sql    |         |          |           |
      |   |   | Country      |        | code    | COUNTRY  | code='lt' |
      |   |   |   | code     | string |         | CODE     |           | open
      |   |   |   | name     | string |         | NAME     |           | open
      |   |   | City         |        | name    | CITY     |           |
      |   |   |   | name     | string |         | NAME     |           | open
      |   |   |   | country  | ref    | Country | COUNTRY  |           | open
      |   |   | District     |        | name    | DISTRICT |           |
      |   |   |   | name     | string |         | NAME     |           | open
      |   |   |   | city     | ref    | City    | CITY     |           | open
    '''))

    # Configure local server with SQL backend
    sqlite.init({
        'COUNTRY': [
            sa.Column('CODE', sa.Text),
            sa.Column('NAME', sa.Text),
        ],
        'CITY': [
            sa.Column('NAME', sa.Text),
            sa.Column('COUNTRY', sa.Text),
        ],
        'DISTRICT': [
            sa.Column('NAME', sa.Text),
            sa.Column('CITY', sa.Text),
        ],
    })
    sqlite.write('COUNTRY', [
        {'NAME': 'Lithuania', 'CODE': 'lt'},
    ])
    sqlite.write('CITY', [
        {'NAME': 'Vilnius', 'COUNTRY': 'lt'},
    ])
    sqlite.write('DISTRICT', [
        {'NAME': 'Old town', 'CITY': 'Vilnius'},
        {'NAME': 'New town', 'CITY': 'Vilnius'},
    ])

    app = create_client(rc, tmp_path, sqlite)
    app.authmodel('example/join/nested', ['search'])
    resp = app.get('/example/join/nested/District?select(city.name, name)')
    assert listdata(resp) == [
        ('Vilnius', 'New town'),
        ('Vilnius', 'Old town'),
    ]


def test_filter_join_array_value(rc, tmp_path, geodb):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
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

    app = create_client(rc, tmp_path, geodb)

    resp = app.get('/datasets/gov/example/country')
    codes = dict(listdata(resp, '_id', 'code'))

    resp = app.get('/datasets/gov/example/city?sort(name)')
    data = listdata(resp, 'country._id', 'name', sort='name')
    data = [(codes.get(country), city) for country, city in data]
    assert data == [
        ('lv', 'Ryga'),
        ('lt', 'Vilnius'),
    ]


def test_filter_join_ne_array_value(rc, tmp_path, geodb):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
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

    app = create_client(rc, tmp_path, geodb)

    resp = app.get('/datasets/gov/example/country')
    codes = dict(listdata(resp, '_id', 'code'))

    resp = app.get('/datasets/gov/example/city?sort(name)')
    data = listdata(resp, 'country._id', 'name', sort='name')
    data = [(codes.get(country), city) for country, city in data]
    assert data == [
        ('ee', 'Talinas'),
    ]


@pytest.mark.skip('todo')
def test_filter_multi_column_pk(rc, tmp_path, geodb):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
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

    app = create_client(rc, tmp_path, geodb)

    resp = app.get('/datasets/gov/example/country')
    codes = dict(listdata(resp, '_id', 'code'))

    resp = app.get('/datasets/gov/example/city?sort(name)')
    data = listdata(resp, 'country._id', 'name', sort='name')
    data = [(codes.get(country), city) for country, city in data]
    assert data == [
        ('lv', 'Ryga'),
        ('lt', 'Vilnius'),
    ]


def test_getall(rc, tmp_path, geodb):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
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

    app = create_client(rc, tmp_path, geodb)

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


def test_select(rc, tmp_path, geodb):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
    id | d | r | b | m | property | source      | prepare | type   | ref     | level | access | uri | title   | description
       | datasets/gov/example     |             |         |        |         |       |        |     | Example |
       |   | data                 |             |         | sql    |         |       |        |     | Data    |
       |   |   |                  |             |         |        |         |       |        |     |         |
       |   |   |   | country      | salis       |         |        | code    |       |        |     | Country |
       |   |   |   |   | code     | kodas       |         | string |         | 3     | open   |     | Code    |
       |   |   |   |   | name     | pavadinimas |         | string |         | 3     | open   |     | Name    |
    '''))

    app = create_client(rc, tmp_path, geodb)

    resp = app.get('/datasets/gov/example/country?select(code,name)')
    assert listdata(resp, 'code', 'name') == [
        ('ee', 'Estija'),
        ('lt', 'Lietuva'),
        ('lv', 'Latvija'),
    ]


@pytest.mark.skip('TODO')
def test_select_len(rc, tmp_path, geodb):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
    id | d | r | b | m | property | source      | prepare | type   | ref     | level | access | uri | title   | description
       | datasets/gov/example     |             |         |        |         |       |        |     | Example |
       |   | data                 |             |         | sql    |         |       |        |     | Data    |
       |   |   |                  |             |         |        |         |       |        |     |         |
       |   |   |   | country      | salis       |         |        | code    |       |        |     | Country |
       |   |   |   |   | code     | kodas       |         | string |         | 3     | open   |     | Code    |
       |   |   |   |   | name     | pavadinimas |         | string |         | 3     | open   |     | Name    |
    '''))

    app = create_client(rc, tmp_path, geodb)

    resp = app.get('/datasets/gov/example/country?select(code,len(name))')
    assert listdata(resp, 'code', 'len(name)') == [
        ('ee', 6),
        ('lt', 7),
        ('lv', 7),
    ]


def test_filter_len(rc, tmp_path, geodb):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
    id | d | r | b | m | property | source      | prepare | type   | ref     | level | access | uri | title   | description
       | datasets/gov/example     |             |         |        |         |       |        |     | Example |
       |   | data                 |             |         | sql    |         |       |        |     | Data    |
       |   |   |                  |             |         |        |         |       |        |     |         |
       |   |   |   | country      | salis       |         |        | code    |       |        |     | Country |
       |   |   |   |   | code     | kodas       |         | string |         | 3     | open   |     | Code    |
       |   |   |   |   | name     | pavadinimas |         | string |         | 3     | open   |     | Name    |
    '''))

    app = create_client(rc, tmp_path, geodb)

    resp = app.get('/datasets/gov/example/country?select(code,name)&len(name)=7&sort(code)')
    assert listdata(resp, 'code', 'name') == [
        ('lt', 'Lietuva'),
        ('lv', 'Latvija'),
    ]


def test_private_property(rc, tmp_path, geodb):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
    id | d | r | b | m | property | source      | prepare    | type   | ref     | level | access  | uri | title   | description
       | datasets/gov/example     |             |            |        |         |       |         |     | Example |
       |   | data                 |             |            | sql    |         |       |         |     | Data    |
       |   |   |                  |             |            |        |         |       |         |     |         |
       |   |   |   | country      | salis       | code!='ee' |        | code    |       |         |     | Country |
       |   |   |   |   | code     | kodas       |            | string |         | 3     | private |     | Code    |
       |   |   |   |   | name     | pavadinimas |            | string |         | 3     | open    |     | Name    |
    '''))

    app = create_client(rc, tmp_path, geodb)

    resp = app.get('/datasets/gov/example/country')
    assert listdata(resp) == [
        'Latvija',
        'Lietuva',
    ]


def test_all_private_properties(rc, tmp_path, geodb):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
    id | d | r | b | m | property | source      | prepare    | type   | ref     | level | access  | uri | title   | description
       | datasets/gov/example     |             |            |        |         |       |         |     | Example |
       |   | data                 |             |            | sql    |         |       |         |     | Data    |
       |   |   |                  |             |            |        |         |       |         |     |         |
       |   |   |   | country      | salis       | code!='ee' |        | code    |       |         |     | Country |
       |   |   |   |   | code     | kodas       |            | string |         | 3     | private |     | Code    |
       |   |   |   |   | name     | pavadinimas |            | string |         | 3     | private |     | Name    |
    '''))

    app = create_client(rc, tmp_path, geodb)

    resp = app.get('/datasets/gov/example/country')
    assert error(resp, status=401) == 'AuthorizedClientsOnly'


def test_default_access(rc, tmp_path, geodb):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
    id | d | r | b | m | property | source      | prepare    | type   | ref     | level | access  | uri | title   | description
       | datasets/gov/example     |             |            |        |         |       |         |     | Example |
       |   | data                 |             |            | sql    |         |       |         |     | Data    |
       |   |   |                  |             |            |        |         |       |         |     |         |
       |   |   |   | country      | salis       | code!='ee' |        | code    |       |         |     | Country |
       |   |   |   |   | code     | kodas       |            | string |         | 3     |         |     | Code    |
       |   |   |   |   | name     | pavadinimas |            | string |         | 3     |         |     | Name    |
    '''))

    app = create_client(rc, tmp_path, geodb)

    resp = app.get('/datasets/gov/example/country')
    assert error(resp, status=401) == 'AuthorizedClientsOnly'


def test_model_open_access(rc, tmp_path, geodb):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
    id | d | r | b | m | property | source      | prepare    | type   | ref     | level | access  | uri | title   | description
       | datasets/gov/example     |             |            |        |         |       |         |     | Example |
       |   | data                 |             |            | sql    |         |       |         |     | Data    |
       |   |   |                  |             |            |        |         |       |         |     |         |
       |   |   |   | country      | salis       | code!='ee' |        | code    |       | open    |     | Country |
       |   |   |   |   | code     | kodas       |            | string |         | 3     |         |     | Code    |
       |   |   |   |   | name     | pavadinimas |            | string |         | 3     |         |     | Name    |
    '''))

    app = create_client(rc, tmp_path, geodb)

    resp = app.get('/datasets/gov/example/country')
    assert listdata(resp) == [
        ('lt', 'Lietuva'),
        ('lv', 'Latvija'),
    ]


def test_property_public_access(rc, tmp_path, geodb):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
    id | d | r | b | m | property | source      | prepare    | type   | ref     | level | access  | uri | title   | description
       | datasets/gov/example     |             |            |        |         |       |         |     | Example |
       |   | data                 |             |            | sql    |         |       |         |     | Data    |
       |   |   |                  |             |            |        |         |       |         |     |         |
       |   |   |   | country      | salis       | code!='ee' |        | code    |       |         |     | Country |
       |   |   |   |   | code     | kodas       |            | string |         | 3     | public  |     | Code    |
       |   |   |   |   | name     | pavadinimas |            | string |         | 3     | open    |     | Name    |
    '''))

    app = create_client(rc, tmp_path, geodb)

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


def test_select_protected_property(rc, tmp_path, geodb):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
    id | d | r | b | m | property | source      | prepare    | type   | ref     | level | access  | uri | title   | description
       | datasets/gov/example     |             |            |        |         |       |         |     | Example |
       |   | data                 |             |            | sql    |         |       |         |     | Data    |
       |   |   |                  |             |            |        |         |       |         |     |         |
       |   |   |   | country      | salis       | code!='ee' |        | code    |       |         |     | Country |
       |   |   |   |   | code     | kodas       |            | string |         | 3     | public  |     | Code    |
       |   |   |   |   | name     | pavadinimas |            | string |         | 3     | open    |     | Name    |
    '''))

    app = create_client(rc, tmp_path, geodb)

    resp = app.get('/datasets/gov/example/country?select(code,name)')
    assert error(resp) == 'PropertyNotFound'

    resp = app.get('/datasets/gov/example/country?select(code,name)', headers={'Accept': 'text/html'})
    assert error(resp) == 'PropertyNotFound'


def test_ns_getall(rc, tmp_path, geodb):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
    id | d | r | b | m | property | source      | prepare    | type   | ref     | level | access  | uri | title   | description
       | datasets/gov/example     |             |            |        |         |       |         |     | Example |
       |   | data                 |             |            | sql    |         |       |         |     | Data    |
       |   |   |                  |             |            |        |         |       |         |     |         |
       |   |   |   | country      | salis       | code!='ee' |        | code    |       |         |     | Country |
       |   |   |   |   | code     | kodas       |            | string |         | 3     | public  |     | Code    |
       |   |   |   |   | name     | pavadinimas |            | string |         | 3     | open    |     | Name    |
    '''))

    app = create_client(rc, tmp_path, geodb)

    resp = app.get('/datasets/gov/example')
    assert listdata(resp, 'name', 'title') == [
        ('datasets/gov/example/country', 'Country'),
    ]

    resp = app.get('/datasets/gov/example', headers={'Accept': 'text/html'})
    assert listdata(resp, 'name', 'title') == [
        ('📄 country', 'Country'),
    ]


def test_push(postgresql, rc, cli: SpintaCliRunner, responses, tmp_path, geodb, request):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
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
    localrc = create_rc(rc, tmp_path, geodb)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == 'https://example.com/'
    result = cli.invoke(localrc, [
        'push',
        '-d', 'datasets/gov/example',
        '-o', remote.url,
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


def test_push_dry_run(postgresql, rc, cli: SpintaCliRunner, responses, tmp_path, geodb, request):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
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
    localrc = create_rc(rc, tmp_path, geodb)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data from local to remote.
    assert remote.url == 'https://example.com/'
    result = cli.invoke(localrc, [
        'push',
        '-d', 'datasets/gov/example',
        '-o', remote.url,
        '--credentials', remote.credsfile,
        '--dry-run',
    ])
    assert result.exit_code == 0

    remote.app.authmodel('datasets/gov/example/Country', ['getall'])
    resp = remote.app.get('/datasets/gov/example/Country')
    assert listdata(resp, 'code', 'name') == []


def test_no_primary_key(rc, tmp_path, geodb):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property | source      | type   | ref | access
    datasets/gov/example     |             |        |     |
      | data                 |             | sql    |     |
      |   |                  |             |        |     |
      |   |   | country      | salis       |        |     |
      |   |   |   | code     | kodas       | string |     | open
      |   |   |   | name     | pavadinimas | string |     | open
    '''))

    app = create_client(rc, tmp_path, geodb)

    resp = app.get('/datasets/gov/example/country')
    codes = dict(listdata(resp, '_id', 'code'))
    data = listdata(resp, '_id', 'code', 'name', sort='code')
    data = [(codes.get(_id), code, name) for _id, code, name in data]
    assert data == [
        ('ee', 'ee', 'Estija'),
        ('lt', 'lt', 'Lietuva'),
        ('lv', 'lv', 'Latvija'),
    ]


def test_count(rc, tmp_path, geodb):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property | source      | type   | ref | access
    datasets/gov/example     |             |        |     |
      | data                 |             | sql    |     |
      |   |                  |             |        |     |
      |   |   | country      | salis       |        |     |
      |   |   |   | code     | kodas       | string |     | open
      |   |   |   | name     | pavadinimas | string |     | open
    '''))

    app = create_client(rc, tmp_path, geodb)

    resp = app.get('/datasets/gov/example/country?select(count())')
    assert listdata(resp) == [3]


def test_push_chunks(
    postgresql,
    rc,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    geodb,
    request,
):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property | source      | type   | ref     | access
    datasets/gov/example     |             |        |         |
      | data                 |             | sql    |         |
      |   |                  |             |        |         |
      |   |   | country      | salis       |        | code    |
      |   |   |   | code     | kodas       | string |         | open
      |   |   |   | name     | pavadinimas | string |         | open
    '''))

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, geodb)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses)
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


def test_push_state(postgresql, rc, cli: SpintaCliRunner, responses, tmp_path, geodb, request):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property | source      | type   | ref     | access
    datasets/gov/example     |             |        |         |
      | data                 |             | sql    |         |
      |   |                  |             |        |         |
      |   |   | country      | salis       |        | code    |
      |   |   |   | code     | kodas       | string |         | open
      |   |   |   | name     | pavadinimas | string |         | open
    '''))

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmp_path, geodb)

    # Configure remote server
    remote = configure_remote_server(cli, localrc, rc, tmp_path, responses)
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
        '--state', tmp_path / 'state.db',
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
        '--state', tmp_path / 'state.db',
    ])

    resp = remote.app.get('/datasets/gov/example/country')
    assert len(listdata(resp)) == 2


def test_prepared_property(rc, tmp_path, geodb):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property  | type   | ref  | source      | prepare | access
    datasets/gov/example      |        |      |             |         |
      | data                  | sql    |      |             |         |
      |   |   | country       |        | code | salis       |         | open
      |   |   |   | code      | string |      | kodas       |         |
      |   |   |   | name      | string |      | pavadinimas |         |
      |   |   |   | continent | string |      |             | 'EU'    |
    '''))

    app = create_client(rc, tmp_path, geodb)

    resp = app.get('/datasets/gov/example/country')
    assert listdata(resp, 'continent', 'code', 'name') == [
        ('EU', 'ee', 'Estija'),
        ('EU', 'lt', 'Lietuva'),
        ('EU', 'lv', 'Latvija'),
    ]


def test_composite_keys(rc, tmp_path, sqlite):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
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

    app = create_client(rc, tmp_path, sqlite)

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


def test_composite_ref_keys(rc, tmp_path, sqlite):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
    d | r | m | property     | type    | ref                     | source       | prepare                 | access
    datasets/ds              |         |                         |              |                         |
      | rs                   | sql     |                         |              |                         |
      |   | Continent        |         | id                      | CONTINENT    |                         | open
      |   |   | id           | integer |                         | ID           |                         |
      |   |   | name         | string  |                         | NAME         |                         |
      |   | Country          |         | id                      | COUNTRY      |                         | open
      |   |   | id           | integer |                         | ID           |                         |
      |   |   | name         | string  |                         | NAME         |                         |
      |   |   | code         | string  |                         | CODE         |                         |
      |   |   | continent    | ref     | Continent               | CONTINENT_ID |                         |
      |   | City             |         | id                      | CITY         |                         | open
      |   |   | id           | integer |                         | ID           |                         |
      |   |   | name         | string  |                         | NAME         |                         |
      |   |   | continent    | ref     | Continent               | CONTINENT_ID |                         |
      |   |   | country_code | string  |                         | COUNTRY_CODE |                         |
      |   |   | country      | ref     | Country[continent,code] |              | continent, country_code |
    '''))

    app = create_client(rc, tmp_path, sqlite)

    sqlite.init({
        'CONTINENT': [
            sa.Column('ID', sa.Integer, primary_key=True),
            sa.Column('NAME', sa.Text),
        ],
        'COUNTRY': [
            sa.Column('ID', sa.Integer, primary_key=True),
            sa.Column('CODE', sa.Text),
            sa.Column('CONTINENT_ID', sa.Integer, sa.ForeignKey('CONTINENT.ID')),
            sa.Column('NAME', sa.Text),
        ],
        'CITY': [
            sa.Column('ID', sa.Integer, primary_key=True),
            sa.Column('CONTINENT_ID', sa.Integer, sa.ForeignKey('CONTINENT.ID')),
            sa.Column('COUNTRY_CODE', sa.Text),
            sa.Column('NAME', sa.Text),
        ],
    })

    sqlite.write('CONTINENT', [
        {'ID': 1, 'NAME': 'Europe'},
        {'ID': 2, 'NAME': 'Africa'},
    ])
    sqlite.write('COUNTRY', [
        {'ID': 1, 'CONTINENT_ID': 1, 'CODE': 'lt', 'NAME': 'Lithuania'},
        {'ID': 2, 'CONTINENT_ID': 1, 'CODE': 'lv', 'NAME': 'Latvia'},
        {'ID': 3, 'CONTINENT_ID': 1, 'CODE': 'ee', 'NAME': 'Estonia'},
    ])
    sqlite.write('CITY', [
        {'ID': 1, 'CONTINENT_ID': 1, 'COUNTRY_CODE': 'lt', 'NAME': 'Vilnius'},
        {'ID': 2, 'CONTINENT_ID': 1, 'COUNTRY_CODE': 'lt', 'NAME': 'Kaunas'},
        {'ID': 3, 'CONTINENT_ID': 1, 'COUNTRY_CODE': 'lv', 'NAME': 'Riga'},
        {'ID': 4, 'CONTINENT_ID': 1, 'COUNTRY_CODE': 'ee', 'NAME': 'Tallinn'},
    ])

    resp = app.get('/datasets/ds/Continent')
    continents = dict(listdata(resp, '_id', 'name'))
    assert sorted(continents.values()) == [
        'Africa',
        'Europe',
    ]

    resp = app.get('/datasets/ds/Country')
    countries = dict(listdata(resp, '_id', 'name'))
    assert sorted(countries.values()) == [
        'Estonia',
        'Latvia',
        'Lithuania',
    ]

    resp = app.get('/datasets/ds/City')
    cities = listdata(resp, 'name', 'country', 'continent', sort='name')
    cities = [
        (
            name,
            countries[country['_id']],
            continents[continent['_id']],
        )
        for name, country, continent in cities
    ]
    assert cities == [
        ('Kaunas', 'Lithuania', 'Europe'),
        ('Riga', 'Latvia', 'Europe'),
        ('Tallinn', 'Estonia', 'Europe'),
        ('Vilnius', 'Lithuania', 'Europe'),
    ]


def test_composite_non_pk_keys(rc, tmp_path, sqlite):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
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

    app = create_client(rc, tmp_path, sqlite)

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


def test_composite_non_pk_keys_with_filter(rc, tmp_path, sqlite):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
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

    app = create_client(rc, tmp_path, sqlite)

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


def test_access_private_primary_key(rc, tmp_path, sqlite):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
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

    app = create_client(rc, tmp_path, sqlite)

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


def test_enum(rc, tmp_path, sqlite):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
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

    app = create_client(rc, tmp_path, sqlite)
    resp = app.get('/datasets/gov/example/Country')
    assert listdata(resp) == [
        ('left', 'India'),
        ('right', 'Latvia'),
        ('right', 'Lithuania'),
    ]


def test_enum_ref(rc, tmp_path, sqlite):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
    d | r | m | property | type   | ref  | source  | prepare | access
                         | enum   | side | l       | 'left'  | open
                         |        |      | r       | 'right' | open
                         |        |      |         |         |
    datasets/gov/example |        |      |         |         |
      | resource         | sql    |      |         |         |
      |   | Country      |        | name | COUNTRY |         |
      |   |   | name     | string |      | NAME    |         | open
      |   |   | driving  | string | side | DRIVING |         | open
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

    app = create_client(rc, tmp_path, sqlite)
    resp = app.get('/datasets/gov/example/Country')
    assert listdata(resp) == [
        ('left', 'India'),
        ('right', 'Latvia'),
        ('right', 'Lithuania'),
    ]


def test_enum_no_prepare(rc, tmp_path, sqlite):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
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

    app = create_client(rc, tmp_path, sqlite)
    resp = app.get('/datasets/gov/example/Country')
    assert listdata(resp) == [
        ('l', 'India'),
        ('r', 'Latvia'),
        ('r', 'Lithuania'),
    ]


def test_enum_empty_source(rc, tmp_path, sqlite):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
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

    app = create_client(rc, tmp_path, sqlite)
    resp = app.get('/datasets/gov/example/Country')
    assert listdata(resp) == [
        ('r', 'Latvia'),
        ('r', 'Lithuania'),
        (None, 'Antarctica'),
    ]


def test_enum_ref_empty_source(rc, tmp_path, sqlite):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
    d | r | m | property     | type   | ref     | source  | prepare       | access
                             | enum   | side    | l       |               | open
                             |        |         | r       |               | open
                             |        |         | -       | null          | open
    datasets/gov/example     |        |         |         |               |
      | resource             | sql    |         |         |               |
      |   | Country          |        | name    | COUNTRY |               |
      |   |   | name         | string |         | NAME    |               | open
      |   |   | driving      | string | side    | DRIVING | swap('', '-') | open
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

    app = create_client(rc, tmp_path, sqlite)
    resp = app.get('/datasets/gov/example/Country')
    assert listdata(resp) == [
        ('r', 'Latvia'),
        ('r', 'Lithuania'),
        (None, 'Antarctica'),
    ]


def test_enum_empty_integer_source(rc, tmp_path, sqlite):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
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

    app = create_client(rc, tmp_path, sqlite)
    resp = app.get('/datasets/gov/example/Country')
    assert listdata(resp) == [
        ('l', 'India'),
        ('r', 'Latvia'),
        ('r', 'Lithuania'),
    ]


def test_filter_by_enum_access(rc, tmp_path, sqlite):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
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

    app = create_client(rc, tmp_path, sqlite)
    resp = app.get('/datasets/gov/example/Country')
    assert listdata(resp) == [
        ('r', 'Latvia'),
        ('r', 'Lithuania'),
    ]


def test_filter_by_ref_enum_access(rc, tmp_path, sqlite):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
    d | r | m | property     | type   | ref     | source  | prepare | access
                             | enum   | side    | 0       | 'l'     | private
                             |        |         | 1       | 'r'     | open
    datasets/gov/example     |        |         |         |         |
      | resource             | sql    |         |         |         |
      |   | Country          |        | name    | COUNTRY |         |
      |   |   | name         | string |         | NAME    |         | open
      |   |   | driving      | string | side    | DRIVING |         | open
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

    app = create_client(rc, tmp_path, sqlite)
    resp = app.get('/datasets/gov/example/Country')
    assert listdata(resp) == [
        ('r', 'Latvia'),
        ('r', 'Lithuania'),
    ]


def test_filter_by_enum(rc, tmp_path, sqlite):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
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

    app = create_client(rc, tmp_path, sqlite)
    resp = app.get('/datasets/gov/example/Country?driving="r"')
    assert listdata(resp) == [
        ('r', 'Latvia'),
        ('r', 'Lithuania'),
    ]


def test_filter_by_ref_enum(rc, tmp_path, sqlite):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
    d | r | m | property     | type   | ref     | source  | prepare | access
                             | enum   | side    | 0       | 'l'     | private
                             |        |         | 1       | 'r'     | open
    datasets/gov/example     |        |         |         |         |
      | resource             | sql    |         |         |         |
      |   | Country          |        | name    | COUNTRY |         |
      |   |   | name         | string |         | NAME    |         | open
      |   |   | driving      | string | side    | DRIVING |         | open
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

    app = create_client(rc, tmp_path, sqlite)
    resp = app.get('/datasets/gov/example/Country?driving="r"')
    assert listdata(resp) == [
        ('r', 'Latvia'),
        ('r', 'Lithuania'),
    ]


def test_filter_by_enum_multi_value(rc, tmp_path, sqlite):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
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

    app = create_client(rc, tmp_path, sqlite)
    resp = app.get('/datasets/gov/example/Country?driving="r"')
    assert listdata(resp) == [
        ('r', 'Latvia'),
        ('r', 'Lithuania'),
    ]


def test_filter_by_enum_list_value(rc, tmp_path, sqlite):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
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

    app = create_client(rc, tmp_path, sqlite)
    resp = app.get('/datasets/gov/example/Country?driving=["l","r"]')
    assert listdata(resp) == [
        ('l', 'India'),
        ('r', 'Latvia'),
        ('r', 'Lithuania'),
    ]


def test_implicit_filter(rc, tmp_path, sqlite):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
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

    app = create_client(rc, tmp_path, sqlite)
    resp = app.get('/datasets/gov/example/City')
    assert listdata(resp, 'name') == [
        'Ryga',
        'Vilnius',
    ]


def test_implicit_filter_no_external_source(rc, tmp_path, sqlite):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
    d | r | m | property     | type   | ref     | source    | prepare     | access
    datasets/gov/example     |        |         |           |             |
      | resource             | sql    |         |           |             |
      |   | Country          |        | code    | COUNTRY   | code = 'lt' |
      |   |   | code         | string |         | CODE      |             | open
      |   |   | name         | string |         | NAME      |             | open
                             |        |         |           |             |
      |   | City             |        | name    | CITY      |             |
      |   |   | name         | string |         | NAME      |             | open
      |   |   | country      | ref    | Country |           |             | open
    '''))

    sqlite.init({
        'COUNTRY': [
            sa.Column('CODE', sa.String),
            sa.Column('NAME', sa.Text),
        ],
        'CITY': [
            sa.Column('NAME', sa.Text),
            sa.Column('COUNTRY', sa.String),
        ],
    })

    sqlite.write('COUNTRY', [
        {'CODE': 'lt', 'NAME': 'Lithuania'},
        {'CODE': 'lv', 'NAME': 'Latvia'},
    ])

    sqlite.write('CITY', [
        {'COUNTRY': 'lt', 'NAME': 'Vilnius'},
        {'COUNTRY': 'lv', 'NAME': 'Ryga'},
    ])

    app = create_client(rc, tmp_path, sqlite)
    resp = app.get('/datasets/gov/example/City')
    assert listdata(resp, 'name') == [
        'Ryga',
        'Vilnius',
    ]


def test_implicit_filter_two_refs(rc, tmp_path, sqlite):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
    d | r | b | m | property     | type    | ref                | source             | prepare | access
    example/standards            |         |                    |                    |         |
      | sql                      | sql     |                    | sqlite://          |         |
                                 |         |                    |                    |         |
      |   |   | StandardDocument |         | standard, document | STANDARD_DOCUMENTS |         |
      |   |   |   | standard     | ref     | Standard           | STANDARD_ID        |         | open
      |   |   |   | document     | ref     | Document           | DOCUMENT_ID        |         | open
                                 |         |                    |                    |         |
      |   |   | Standard         |         | id                 | STANDARD           |         |
      |   |   |   | id           | integer |                    | STANDARD_ID        |         | private
      |   |   |   | name         | string  |                    | STANDARD_NAME      |         | open
                                 |         |                    |                    |         |
      |   |   | Document         |         | id                 | DOCUMENT           | id=2    |
      |   |   |   | id           | integer |                    | DOCUMENT_ID        |         | private
      |   |   |   | name         | string  |                    | NAME               |         | open
    '''))

    sqlite.init({
        'STANDARD': [
            sa.Column('STANDARD_ID', sa.Integer),
            sa.Column('STANDARD_NAME', sa.Text),
        ],
        'DOCUMENT': [
            sa.Column('DOCUMENT_ID', sa.Integer),
            sa.Column('NAME', sa.Text),
        ],
        'STANDARD_DOCUMENTS': [
            sa.Column('STANDARD_ID', sa.Integer, sa.ForeignKey('STANDARD.STANDARD_ID')),
            sa.Column('DOCUMENT_ID', sa.Integer, sa.ForeignKey('DOCUMENT.DOCUMENT_ID')),
        ],
    })

    sqlite.write('STANDARD', [
        {'STANDARD_ID': 1, 'STANDARD_NAME': 'S1'},
        {'STANDARD_ID': 2, 'STANDARD_NAME': 'S2'},
    ])

    sqlite.write('DOCUMENT', [
        {'DOCUMENT_ID': 1, 'NAME': 'DOC1'},
        {'DOCUMENT_ID': 2, 'NAME': 'DOC2'},
    ])

    sqlite.write('STANDARD_DOCUMENTS', [
        {'STANDARD_ID': 1, 'DOCUMENT_ID': 1},
        {'STANDARD_ID': 2, 'DOCUMENT_ID': 2},
    ])

    app = create_client(rc, tmp_path, sqlite)

    resp = app.get(
        '/example/standards/StandardDocument'
        '?select(standard.name, document.name)'
    )
    assert listdata(resp, 'standard.name', 'document.name') == [
        ('S2', 'DOC2'),
    ]


def test_implicit_filter_by_enum(rc, tmp_path, sqlite):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
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

    app = create_client(rc, tmp_path, sqlite)
    resp = app.get('/datasets/gov/example/City')
    assert listdata(resp, 'name') == [
        'Ryga',
        'Vilnius',
    ]


def test_implicit_filter_by_enum_empty_access(rc, tmp_path, sqlite):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
    d | r | m | property     | type   | ref     | source    | prepare          | access
    datasets/gov/example     |        |         |           |                  |
                             | enum   | Side    | 0         | 'l'              |
                             |        |         | 1         | 'r'              |
                             |        |         |           |                  |
      | resource             | sql    |         |           |                  |
      |   | Country          |        | id      | COUNTRY   | continent = "eu" |
      |   |   | id           | string |         | ID        |                  | private
      |   |   | name         | string |         | NAME      |                  | open
      |   |   | continent    | string |         | CONTINENT |                  | open
      |   |   | driving      | string | Side    | DRIVING   |                  | open
                             |        |         |           |                  |
      |   | City             |        | name    | CITY      |                  |
      |   |   | name         | string |         | NAME      |                  | open
      |   |   | country      | ref    | Country | COUNTRY   |                  | open
    '''))

    sqlite.init({
        'COUNTRY': [
            sa.Column('ID', sa.Integer),
            sa.Column('NAME', sa.Text),
            sa.Column('CONTINENT', sa.Text),
            sa.Column('DRIVING', sa.Integer),
        ],
        'CITY': [
            sa.Column('NAME', sa.Text),
            sa.Column('COUNTRY', sa.Integer),
        ],
    })

    sqlite.write('COUNTRY', [
        {'ID': 1, 'CONTINENT': 'az', 'DRIVING': 0, 'NAME': 'India'},
        {'ID': 2, 'CONTINENT': 'eu', 'DRIVING': 1, 'NAME': 'Lithuania'},
        {'ID': 3, 'CONTINENT': 'eu', 'DRIVING': 1, 'NAME': 'Latvia'},
    ])

    sqlite.write('CITY', [
        {'COUNTRY': 1, 'NAME': 'Mumbai'},
        {'COUNTRY': 1, 'NAME': 'Delhi'},
        {'COUNTRY': 2, 'NAME': 'Vilnius'},
        {'COUNTRY': 3, 'NAME': 'Ryga'},
    ])

    app = create_client(rc, tmp_path, sqlite)
    resp = app.get('/datasets/gov/example/City')
    assert listdata(resp, 'name') == [
        'Ryga',
        'Vilnius',
    ]


def test_file(rc, tmp_path, sqlite):
    create_tabular_manifest(tmp_path / 'manifest.csv', '''
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

    app = create_client(rc, tmp_path, sqlite)
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
    tmp_path,
    sqlite: Sqlite,
    request,
):
    create_tabular_manifest(tmp_path / 'manifest.csv', '''
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
    local_rc = create_rc(rc, tmp_path, sqlite)

    # Configure remote server
    remote = configure_remote_server(cli, local_rc, rc, tmp_path, responses)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data to the remote server
    cli.invoke(local_rc, [
        'push',
        str(tmp_path / 'manifest.csv'),
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


def test_image(rc, tmp_path, sqlite):
    create_tabular_manifest(tmp_path / 'manifest.csv', '''
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

    app = create_client(rc, tmp_path, sqlite)
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
    tmp_path,
    sqlite: Sqlite,
    request,
):
    create_tabular_manifest(tmp_path / 'manifest.csv', '''
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
    local_rc = create_rc(rc, tmp_path, sqlite)

    # Configure remote server
    remote = configure_remote_server(cli, local_rc, rc, tmp_path, responses)
    request.addfinalizer(remote.app.context.wipe_all)

    # Push data to the remote server
    cli.invoke(local_rc, [
        'push',
        str(tmp_path / 'manifest.csv'),
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
    tmp_path,
    sqlite: Sqlite,
    request,
):
    create_tabular_manifest(tmp_path / 'manifest.csv', '''
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
    local_rc = create_rc(rc, tmp_path, sqlite)

    # Configure remote server
    remote = configure_remote_server(cli, local_rc, rc, tmp_path, responses)
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
    assert listdata(resp, full=True, sort='name') == [
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
    tmp_path,
    sqlite: Sqlite,
    request,
):
    create_tabular_manifest(tmp_path / 'manifest.csv', '''
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
    local_rc = create_rc(rc, tmp_path, sqlite)

    # Configure remote server
    remote = configure_remote_server(cli, local_rc, rc, tmp_path, responses)
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


def _prep_error_handling(
    tmp_path: pathlib.Path,
    sqlite: Sqlite,
    rc: RawConfig,
    responses: RequestsMock,
    *,
    response: Tuple[int, Dict[str, str], str] = None,
    exception: Exception = None,
) -> RawConfig:
    create_tabular_manifest(tmp_path / 'manifest.csv', '''
    d | r | b | m | property      | type     | ref          | source        | access
    example/errors                |          |              |               |
      | resource                  | sql      | sql          |               |
      |   |   | City              |          | id           | CITY          |
      |   |   |   | id            | integer  |              | ID            | private
      |   |   |   | name          | string   |              | NAME          | open
    ''')

    # Configure local server with SQL backend
    sqlite.init({
        'CITY': [
            sa.Column('ID',           sa.Integer),
            sa.Column('NAME',         sa.String),
        ],
    })
    sqlite.write('CITY', [
        {'ID': 1, 'NAME': 'Vilnius'},
        {'ID': 2, 'NAME': 'Kaunas'},
    ])
    rc = create_rc(rc, tmp_path, sqlite)

    # Configure remote server
    def handler(request: PreparedRequest):
        if request.url.endswith('/auth/token'):
            return (
                200,
                {'content-type': 'application/json'},
                '{"access_token":"TOKEN"}',
            )
        elif exception:
            raise exception
        else:
            return response

    responses.add_callback(
        POST, re.compile(r'https://example.com/.*'),
        callback=handler,
        content_type='application/json',
    )

    add_client_credentials(
        tmp_path / 'credentials.cfg',
        'https://example.com',
        client='test',
        secret='secret',
        scopes=['spinta_insert'],
    )

    return rc


def test_error_handling_server_error(
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses: RequestsMock,
    tmp_path: pathlib.Path,
    sqlite: Sqlite,
    caplog: LogCaptureFixture,
):
    rc = _prep_error_handling(tmp_path, sqlite, rc, responses, response=(
        400,
        {'content-type': 'application/json'},
        '{"errors":[{"type": "system", "message": "ERROR"}]}',
    ))

    # Push data from local to remote.
    with caplog.at_level(logging.ERROR):
        cli.invoke(rc, [
            'push',
            '-o', 'spinta+https://example.com',
            '--credentials', tmp_path / 'credentials.cfg',
        ], fail=False)

    message = (
        'Error when sending and receiving data. Model example/errors/City, '
        'items in chunk: 2, first item in chunk:'
    )
    assert message in caplog.text
    assert 'Server response (status=400):' in caplog.text


def test_error_handling_io_error(
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses: RequestsMock,
    tmp_path,
    sqlite: Sqlite,
    caplog: LogCaptureFixture,
):
    rc = _prep_error_handling(
        tmp_path, sqlite, rc, responses,
        exception=IOError('I/O error.'),
    )

    # Push data from local to remote.
    with caplog.at_level(logging.ERROR):
        cli.invoke(rc, [
            'push',
            '-o', 'spinta+https://example.com',
            '--credentials', tmp_path / 'credentials.cfg',
        ], fail=False)

    message = (
        'Error when sending and receiving data. Model example/errors/City, '
        'items in chunk: 2, first item in chunk:'
    )
    assert message in caplog.text
    assert 'Error: I/O error.' in caplog.text


def test_sql_views(rc: RawConfig, tmp_path: pathlib.Path, sqlite: Sqlite):
    create_tabular_manifest(tmp_path / 'manifest.csv', '''
    d | r | b | m | property      | type     | ref          | source        | access
    example/views                 |          |              |               |
      | resource                  | sql      | sql          |               |
      |   |   | City              |          | id           | CITY_VIEW     |
      |   |   |   | id            | integer  |              | ID            | private
      |   |   |   | name          | string   |              | NAME          | open
    ''')

    # Configure local server with SQL backend
    sqlite.init({
        'CITY': [
            sa.Column('ID', sa.Integer),
            sa.Column('NAME', sa.String),
        ],
    })
    sqlite.write('CITY', [
        {'ID': 1, 'NAME': 'Vilnius'},
        {'ID': 2, 'NAME': 'Kaunas'},
    ])
    sqlite.engine.execute(
        'CREATE VIEW CITY_VIEW AS '
        'SELECT * FROM CITY'
    )

    app = create_client(rc, tmp_path, sqlite)

    resp = app.get('/example/views/City')
    assert listdata(resp) == ['Kaunas', 'Vilnius']


@pytest.mark.skip('TODO')
def test_params(
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    sqlite: Sqlite,
):
    create_tabular_manifest(tmp_path / 'manifest.csv', '''
    d | r | b | m | property | type    | ref      | source   | prepare
    example/self/ref/param   |         |          |          |
      | resource             | sql     | sql      |          |
      |   |   | Category     |         | id       | CATEGORY | parent = param(parent)
      |   |   |   |          | param   | parent   |          | null
      |   |   |   |          |         |          | Category | select(id).id
      |   |   |   | id       | integer |          | ID       |
      |   |   |   | name     | string  |          | NAME     |
      |   |   |   | parent   | ref     | Category | PARENT   |
    ''')

    # Configure local server with SQL backend
    sqlite.init({
        'CATEGORY': [
            sa.Column('ID',     sa.Integer),
            sa.Column('NAME',   sa.String),
            sa.Column('PARENT', sa.Integer, sa.ForeignKey('CATEGORY.ID')),
        ],
    })
    sqlite.write('CATEGORY', [
        {
            'ID': 1,
            'NAME': 'Cat 1',
            'PARENT': None,
        },
        {
            'ID': 2,
            'NAME': 'Cat 1.1',
            'PARENT': 1,
        },
    ])

    app = create_client(rc, tmp_path, sqlite)
    app.authmodel('example/self/ref/param', ['search'])

    resp = app.get('/example/self/ref/param/Category?select(name)')
    assert listdata(resp, sort=False) == ['Cat 1', 'Cat 1.1']


def test_cast_string(
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    sqlite: Sqlite,
):
    dataset = 'example/func/cast/string'
    create_tabular_manifest(tmp_path / 'manifest.csv', f'''
    d | r | b | m | property  | type    | ref      | source   | prepare
    {dataset}                 |         |          |          |
      | resource              | sql     | sql      |          |
      |   |   | Data          |         | id       | DATA     |
      |   |   |   | id        | string  |          | ID       | cast()
    ''')

    # Configure local server with SQL backend
    sqlite.init({
        'DATA': [
            sa.Column('ID', sa.Integer),
        ],
    })
    sqlite.write('DATA', [{'ID': 1}])

    app = create_client(rc, tmp_path, sqlite)
    app.authmodel(dataset, ['getall'])

    resp = app.get(f'/{dataset}/Data')
    assert listdata(resp) == ['1']


def test_cast_integer(
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    sqlite: Sqlite,
):
    dataset = 'example/func/cast/integer'
    create_tabular_manifest(tmp_path / 'manifest.csv', f'''
    d | r | b | m | property  | type    | ref      | source   | prepare
    {dataset}                 |         |          |          |
      | resource              | sql     | sql      |          |
      |   |   | Data          |         | id       | DATA     |
      |   |   |   | id        | integer |          | ID       | cast()
    ''')

    # Configure local server with SQL backend
    sqlite.init({
        'DATA': [
            sa.Column('ID', sa.Float),
        ],
    })
    sqlite.write('DATA', [{'ID': 1.0}])

    app = create_client(rc, tmp_path, sqlite)
    app.authmodel(dataset, ['getall'])

    resp = app.get(f'/{dataset}/Data')
    assert listdata(resp) == [1]


def test_cast_integer_error(
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    sqlite: Sqlite,
):
    dataset = 'example/func/cast/integer/error'
    create_tabular_manifest(tmp_path / 'manifest.csv', f'''
    d | r | b | m | property  | type    | ref      | source   | prepare
    {dataset}                 |         |          |          |
      | resource              | sql     | sql      |          |
      |   |   | Data          |         | id       | DATA     |
      |   |   |   | id        | integer |          | ID       | cast()
    ''')

    # Configure local server with SQL backend
    sqlite.init({
        'DATA': [
            sa.Column('ID', sa.Float),
        ],
    })
    sqlite.write('DATA', [{'ID': 1.1}])

    app = create_client(rc, tmp_path, sqlite)
    app.authmodel(dataset, ['getall'])

    resp = app.get(f'/{dataset}/Data')
    assert error(resp) == 'UnableToCast'


def test_point(
    postgresql,
    rc: RawConfig,
    cli: SpintaCliRunner,
    responses,
    tmp_path,
    sqlite: Sqlite,
):
    dataset = 'example/func/point'
    create_tabular_manifest(tmp_path / 'manifest.csv', f'''
    d | r | b | m | property | type     | ref | source | prepare     | access
    {dataset}                |          |     |        |             |
      | resource             | sql      | sql |        |             |
      |   |   | Data         |          | id  | data   |             |
      |   |   |   | id       | integer  |     | id     |             | open
      |   |   |   | x        | number   |     | x      |             | private
      |   |   |   | y        | number   |     | y      |             | private
      |   |   |   | point    | geometry |     |        | point(x, y) | open
    ''')

    # Configure local server with SQL backend
    sqlite.init({
        'data': [
            sa.Column('id', sa.Integer),
            sa.Column('x', sa.Float),
            sa.Column('y', sa.Float),
        ],
    })
    sqlite.write('data', [{
        'id': 1,
        'x': 4,
        'y': 2,
    }])

    app = create_client(rc, tmp_path, sqlite)
    app.authmodel(dataset, ['getall'])

    resp = app.get(f'/{dataset}/Data')
    assert listdata(resp) == [(1, 'POINT (4.0 2.0)')]
