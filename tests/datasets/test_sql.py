import pathlib
import os
import tempfile

import pytest

import sqlalchemy as sa

from spinta.cli import push
from spinta.core.config import RawConfig
from spinta.testing.data import listdata
from spinta.testing.client import create_test_client
from spinta.testing.context import create_test_context
from spinta.testing.tabular import striptable
from spinta.testing.tabular import create_tabular_manifest
from spinta.testing.utils import error
from spinta.testing.client import create_remote_server


class Sqlite:

    def __init__(self, dsn: str):
        self.dsn = dsn
        self.engine = sa.create_engine(dsn)
        self.schema = sa.MetaData(self.engine)

        self.tables = {
            'country': sa.Table(
                'salis', self.schema,
                sa.Column('id', sa.Integer, primary_key=True),
                sa.Column('kodas', sa.Text),
                sa.Column('pavadinimas', sa.Text),
            ),
            'city': sa.Table(
                'miestas', self.schema,
                sa.Column('id', sa.Integer, primary_key=True),
                sa.Column('pavadinimas', sa.Text),
                sa.Column('salis', sa.Text),
            )
        }

    def write(self, table, data):
        table = self.tables[table]
        with self.engine.begin() as conn:
            conn.execute(table.insert(), data)


@pytest.fixture(scope='module')
def sqlite():
    with tempfile.TemporaryDirectory() as tmpdir:
        db = Sqlite('sqlite:///' + os.path.join(tmpdir, 'db.sqlite'))
        db.schema.create_all()
        db.write('country', [
            {'kodas': 'lt', 'pavadinimas': 'Lietuva'},
            {'kodas': 'lv', 'pavadinimas': 'Latvija'},
            {'kodas': 'ee', 'pavadinimas': 'Estija'},
        ])
        db.write('city', [
            {'salis': 'lt', 'pavadinimas': 'Vilnius'},
            {'salis': 'lv', 'pavadinimas': 'Ryga'},
            {'salis': 'ee', 'pavadinimas': 'Talinas'},
        ])
        yield db


def create_rc(rc: RawConfig, tmpdir: pathlib.Path, sqlite: Sqlite):
    return rc.fork({
        'manifests': {
            'default': {
                'type': 'tabular',
                'path': str(tmpdir / 'manifest.csv'),
                'backend': 'sql',
            },
        },
        'backends': {
            'sql': {
                'type': 'sql',
                'dsn': sqlite.dsn,
            },
        },
        # tests/config/clients/3388ea36-4a4f-4821-900a-b574c8829d52.yml
        'default_auth_client': '3388ea36-4a4f-4821-900a-b574c8829d52',
    })


def create_client(rc: RawConfig, tmpdir: pathlib.Path, sqlite: Sqlite):
    rc = create_rc(rc, tmpdir, sqlite)
    context = create_test_context(rc)
    return create_test_client(context)


def test_filter(rc, tmpdir, sqlite):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    id | d | r | b | m | property | source      | prepare   | type   | ref     | level | access | uri | title   | description
       | datasets/gov/example     |             |           |        |         |       |        |     | Example |
       |   | data                 |             |           | sql    |         |       |        |     | Data    |
       |   |   |                  |             |           |        |         |       |        |     |         |
       |   |   |   | country      | salis       | code='lt' |        | code    |       |        |     | Country |
       |   |   |   |   | code     | kodas       |           | string |         | 3     | open   |     | Code    |
       |   |   |   |   | name     | pavadinimas |           | string |         | 3     | open   |     | Name    |
    '''))

    app = create_client(rc, tmpdir, sqlite)

    resp = app.get('/datasets/gov/example/country')
    assert listdata(resp, 'code', 'name') == [
        ('lt', 'Lietuva'),
    ]


def test_filter_join(rc, tmpdir, sqlite):
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

    app = create_client(rc, tmpdir, sqlite)

    resp = app.get('/datasets/gov/example/city?sort(name)')
    assert listdata(resp, 'country', 'name') == [
        ({'_id': 'lt'}, 'Vilnius'),
    ]


def test_filter_join_array_value(rc, tmpdir, sqlite):
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

    app = create_client(rc, tmpdir, sqlite)

    resp = app.get('/datasets/gov/example/city?sort(name)')
    assert listdata(resp, 'country', 'name', sort='name') == [
        ({'_id': 'lv'}, 'Ryga'),
        ({'_id': 'lt'}, 'Vilnius'),
    ]


def test_filter_join_ne_array_value(rc, tmpdir, sqlite):
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

    app = create_client(rc, tmpdir, sqlite)

    resp = app.get('/datasets/gov/example/city?sort(name)')
    assert listdata(resp, 'country', 'name', sort='name') == [
        ({'_id': 'ee'}, 'Talinas'),
    ]


def test_filter_multi_column_pk(rc, tmpdir, sqlite):
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

    app = create_client(rc, tmpdir, sqlite)

    resp = app.get('/datasets/gov/example/city?sort(name)')
    assert listdata(resp, 'country', 'name', sort='name') == [
        ({'_id': 'lv'}, 'Ryga'),
        ({'_id': 'lt'}, 'Vilnius'),
    ]


def test_getall(rc, tmpdir, sqlite):
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

    app = create_client(rc, tmpdir, sqlite)

    resp = app.get('/datasets/gov/example/country?sort(code)')
    assert listdata(resp, 'code', 'name', '_type') == [
        ('ee', 'Estija', 'datasets/gov/example/country'),
        ('lt', 'Lietuva', 'datasets/gov/example/country'),
        ('lv', 'Latvija', 'datasets/gov/example/country'),
    ]

    resp = app.get('/datasets/gov/example/city?sort(name)')
    assert listdata(resp, 'country', 'name', '_type', sort='name') == [
        ({'_id': 'lv'}, 'Ryga', 'datasets/gov/example/city'),
        ({'_id': 'ee'}, 'Talinas', 'datasets/gov/example/city'),
        ({'_id': 'lt'}, 'Vilnius', 'datasets/gov/example/city'),
    ]


def test_select(rc, tmpdir, sqlite):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    id | d | r | b | m | property | source      | prepare | type   | ref     | level | access | uri | title   | description
       | datasets/gov/example     |             |         |        |         |       |        |     | Example |
       |   | data                 |             |         | sql    |         |       |        |     | Data    |
       |   |   |                  |             |         |        |         |       |        |     |         |
       |   |   |   | country      | salis       |         |        | code    |       |        |     | Country |
       |   |   |   |   | code     | kodas       |         | string |         | 3     | open   |     | Code    |
       |   |   |   |   | name     | pavadinimas |         | string |         | 3     | open   |     | Name    |
    '''))

    app = create_client(rc, tmpdir, sqlite)

    resp = app.get('/datasets/gov/example/country?select(code,name)')
    assert listdata(resp, 'code', 'name') == [
        ('ee', 'Estija'),
        ('lt', 'Lietuva'),
        ('lv', 'Latvija'),
    ]


@pytest.mark.skip('TODO')
def test_select_len(rc, tmpdir, sqlite):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    id | d | r | b | m | property | source      | prepare | type   | ref     | level | access | uri | title   | description
       | datasets/gov/example     |             |         |        |         |       |        |     | Example |
       |   | data                 |             |         | sql    |         |       |        |     | Data    |
       |   |   |                  |             |         |        |         |       |        |     |         |
       |   |   |   | country      | salis       |         |        | code    |       |        |     | Country |
       |   |   |   |   | code     | kodas       |         | string |         | 3     | open   |     | Code    |
       |   |   |   |   | name     | pavadinimas |         | string |         | 3     | open   |     | Name    |
    '''))

    app = create_client(rc, tmpdir, sqlite)

    resp = app.get('/datasets/gov/example/country?select(code,len(name))')
    assert listdata(resp, 'code', 'len(name)') == [
        ('ee', 6),
        ('lt', 7),
        ('lv', 7),
    ]


def test_filter_len(rc, tmpdir, sqlite):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    id | d | r | b | m | property | source      | prepare | type   | ref     | level | access | uri | title   | description
       | datasets/gov/example     |             |         |        |         |       |        |     | Example |
       |   | data                 |             |         | sql    |         |       |        |     | Data    |
       |   |   |                  |             |         |        |         |       |        |     |         |
       |   |   |   | country      | salis       |         |        | code    |       |        |     | Country |
       |   |   |   |   | code     | kodas       |         | string |         | 3     | open   |     | Code    |
       |   |   |   |   | name     | pavadinimas |         | string |         | 3     | open   |     | Name    |
    '''))

    app = create_client(rc, tmpdir, sqlite)

    resp = app.get('/datasets/gov/example/country?select(code,name)&len(name)=7&sort(code)')
    assert listdata(resp, 'code', 'name') == [
        ('lt', 'Lietuva'),
        ('lv', 'Latvija'),
    ]


def test_private_property(rc, tmpdir, sqlite):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    id | d | r | b | m | property | source      | prepare    | type   | ref     | level | access  | uri | title   | description
       | datasets/gov/example     |             |            |        |         |       |         |     | Example |
       |   | data                 |             |            | sql    |         |       |         |     | Data    |
       |   |   |                  |             |            |        |         |       |         |     |         |
       |   |   |   | country      | salis       | code!='ee' |        | code    |       |         |     | Country |
       |   |   |   |   | code     | kodas       |            | string |         | 3     | private |     | Code    |
       |   |   |   |   | name     | pavadinimas |            | string |         | 3     | open    |     | Name    |
    '''))

    app = create_client(rc, tmpdir, sqlite)

    resp = app.get('/datasets/gov/example/country')
    assert listdata(resp) == [
        'Latvija',
        'Lietuva',
    ]


def test_all_private_properties(rc, tmpdir, sqlite):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    id | d | r | b | m | property | source      | prepare    | type   | ref     | level | access  | uri | title   | description
       | datasets/gov/example     |             |            |        |         |       |         |     | Example |
       |   | data                 |             |            | sql    |         |       |         |     | Data    |
       |   |   |                  |             |            |        |         |       |         |     |         |
       |   |   |   | country      | salis       | code!='ee' |        | code    |       |         |     | Country |
       |   |   |   |   | code     | kodas       |            | string |         | 3     | private |     | Code    |
       |   |   |   |   | name     | pavadinimas |            | string |         | 3     | private |     | Name    |
    '''))

    app = create_client(rc, tmpdir, sqlite)

    resp = app.get('/datasets/gov/example/country')
    assert error(resp, status=401) == 'AuthorizedClientsOnly'


def test_default_access(rc, tmpdir, sqlite):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    id | d | r | b | m | property | source      | prepare    | type   | ref     | level | access  | uri | title   | description
       | datasets/gov/example     |             |            |        |         |       |         |     | Example |
       |   | data                 |             |            | sql    |         |       |         |     | Data    |
       |   |   |                  |             |            |        |         |       |         |     |         |
       |   |   |   | country      | salis       | code!='ee' |        | code    |       |         |     | Country |
       |   |   |   |   | code     | kodas       |            | string |         | 3     |         |     | Code    |
       |   |   |   |   | name     | pavadinimas |            | string |         | 3     |         |     | Name    |
    '''))

    app = create_client(rc, tmpdir, sqlite)

    resp = app.get('/datasets/gov/example/country')
    assert error(resp, status=401) == 'AuthorizedClientsOnly'


def test_model_open_access(rc, tmpdir, sqlite):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    id | d | r | b | m | property | source      | prepare    | type   | ref     | level | access  | uri | title   | description
       | datasets/gov/example     |             |            |        |         |       |         |     | Example |
       |   | data                 |             |            | sql    |         |       |         |     | Data    |
       |   |   |                  |             |            |        |         |       |         |     |         |
       |   |   |   | country      | salis       | code!='ee' |        | code    |       | open    |     | Country |
       |   |   |   |   | code     | kodas       |            | string |         | 3     |         |     | Code    |
       |   |   |   |   | name     | pavadinimas |            | string |         | 3     |         |     | Name    |
    '''))

    app = create_client(rc, tmpdir, sqlite)

    resp = app.get('/datasets/gov/example/country')
    assert listdata(resp) == [
        ('lt', 'Lietuva'),
        ('lv', 'Latvija'),
    ]


def test_property_public_access(rc, tmpdir, sqlite):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    id | d | r | b | m | property | source      | prepare    | type   | ref     | level | access  | uri | title   | description
       | datasets/gov/example     |             |            |        |         |       |         |     | Example |
       |   | data                 |             |            | sql    |         |       |         |     | Data    |
       |   |   |                  |             |            |        |         |       |         |     |         |
       |   |   |   | country      | salis       | code!='ee' |        | code    |       |         |     | Country |
       |   |   |   |   | code     | kodas       |            | string |         | 3     | public  |     | Code    |
       |   |   |   |   | name     | pavadinimas |            | string |         | 3     | open    |     | Name    |
    '''))

    app = create_client(rc, tmpdir, sqlite)

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


def test_select_protected_property(rc, tmpdir, sqlite):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    id | d | r | b | m | property | source      | prepare    | type   | ref     | level | access  | uri | title   | description
       | datasets/gov/example     |             |            |        |         |       |         |     | Example |
       |   | data                 |             |            | sql    |         |       |         |     | Data    |
       |   |   |                  |             |            |        |         |       |         |     |         |
       |   |   |   | country      | salis       | code!='ee' |        | code    |       |         |     | Country |
       |   |   |   |   | code     | kodas       |            | string |         | 3     | public  |     | Code    |
       |   |   |   |   | name     | pavadinimas |            | string |         | 3     | open    |     | Name    |
    '''))

    app = create_client(rc, tmpdir, sqlite)

    resp = app.get('/datasets/gov/example/country?select(code,name)')
    assert error(resp) == 'PropertyNotFound'

    resp = app.get('/datasets/gov/example/country?select(code,name)', headers={'Accept': 'text/html'})
    assert error(resp) == 'PropertyNotFound'


def test_ns_getall(rc, tmpdir, sqlite):
    create_tabular_manifest(tmpdir / 'manifest.csv', striptable('''
    id | d | r | b | m | property | source      | prepare    | type   | ref     | level | access  | uri | title   | description
       | datasets/gov/example     |             |            |        |         |       |         |     | Example |
       |   | data                 |             |            | sql    |         |       |         |     | Data    |
       |   |   |                  |             |            |        |         |       |         |     |         |
       |   |   |   | country      | salis       | code!='ee' |        | code    |       |         |     | Country |
       |   |   |   |   | code     | kodas       |            | string |         | 3     | public  |     | Code    |
       |   |   |   |   | name     | pavadinimas |            | string |         | 3     | open    |     | Name    |
    '''))

    app = create_client(rc, tmpdir, sqlite)

    resp = app.get('/datasets/gov/example')
    assert listdata(resp, '_id', 'title') == [
        ('datasets/gov/example/country', 'Country'),
    ]

    resp = app.get('/datasets/gov/example', headers={'Accept': 'text/html'})
    assert listdata(resp, '_id', 'title') == [
        ('datasets/gov/example/country', 'Country'),
    ]


def test_push(mongo, rc, cli, responses, tmpdir, sqlite):
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

    # Create remote server with Mongo backend
    tmpdir = pathlib.Path(tmpdir)
    remoterc = rc.fork({
        'manifests': {
            'default': {
                'type': 'tabular',
                'path': str(tmpdir / 'manifest.csv'),
                'backend': 'mongo',
            },
        },
        'backends': ['mongo'],
    })
    remote = create_remote_server(
        remoterc,
        tmpdir,
        responses,
        scopes=['spinta_upsert'],
        credsfile=True,
    )

    # Configure local server with SQL backend
    localrc = create_rc(rc, tmpdir, sqlite)

    # Push data from local to remote.
    cli.invoke(localrc, push, [
        remote.url,
        '-r', str(remote.credsfile),
        '-c', remote.client,
        '-d', 'datasets/gov/example',
    ])

    remote.app.authmodel('datasets/gov/example/country', ['getall'])
    resp = remote.app.get('/datasets/gov/example/country')
    assert listdata(resp, 'code', 'name') == [
        ('ee', 'Estija'),
        ('lt', 'Lietuva'),
        ('lv', 'Latvija')
    ]

    remote.app.authmodel('datasets/gov/example/city', ['getall'])
    resp = remote.app.get('/datasets/gov/example/city')
    assert listdata(resp, 'country._id', 'name') == [
        (None, 'Ryga'),
        (None, 'Talinas'),
        (None, 'Vilnius'),
    ]

    # Add new data to local server
    sqlite.write('city', [
        {'salis': 'lt', 'pavadinimas': 'Kaunas'},
    ])

    # Push data from local to remote.
    cli.invoke(localrc, push, [
        remote.url,
        '-r', str(remote.credsfile),
        '-c', remote.client,
        '-d', 'datasets/gov/example',
    ])

    resp = remote.app.get('/datasets/gov/example/country')
    assert listdata(resp, 'code', 'name') == [
        ('ee', 'Estija'),
        ('lt', 'Lietuva'),
        ('lv', 'Latvija')
    ]

    resp = remote.app.get('/datasets/gov/example/city')
    assert listdata(resp, 'country._id', 'name') == [
        (None, 'Kaunas'),
        (None, 'Ryga'),
        (None, 'Talinas'),
        (None, 'Vilnius'),
    ]
