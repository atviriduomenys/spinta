import pathlib
import os
import tempfile

import pytest

import sqlalchemy as sa

from spinta.core.config import RawConfig
from spinta.testing.data import listdata
from spinta.testing.client import create_test_client
from spinta.testing.context import create_test_context
from spinta.testing.tabular import striptable
from spinta.testing.tabular import create_tabular_manifest
from spinta.testing.utils import error


@pytest.fixture(scope='module')
def sqlite():
    with tempfile.TemporaryDirectory() as tmpdir:
        dsn = 'sqlite:///' + os.path.join(tmpdir, 'db.sqlite')
        engine = sa.create_engine(dsn)
        schema = sa.MetaData(engine)

        country = sa.Table(
            'salis', schema,
            sa.Column('id', sa.Integer, primary_key=True),
            sa.Column('kodas', sa.Text),
            sa.Column('pavadinimas', sa.Text),
        )

        city = sa.Table(
            'miestas', schema,
            sa.Column('id', sa.Integer, primary_key=True),
            sa.Column('pavadinimas', sa.Text),
            sa.Column('salis', sa.Text),
        )

        schema.create_all()

        with engine.begin() as conn:
            conn.execute(country.insert(), [
                {'kodas': 'lt', 'pavadinimas': 'Lietuva'},
                {'kodas': 'lv', 'pavadinimas': 'Latvija'},
                {'kodas': 'ee', 'pavadinimas': 'Estija'},
            ])
            conn.execute(city.insert(), [
                {'salis': 'lt', 'pavadinimas': 'Vilnius'},
                {'salis': 'lv', 'pavadinimas': 'Ryga'},
                {'salis': 'ee', 'pavadinimas': 'Talinas'},
            ])

        yield dsn


def create_client(rc: RawConfig, tmpdir: pathlib.Path, sqlite):
    rc = rc.fork({
        'manifests.default': {
            'type': 'tabular',
            'path': str(tmpdir / 'manifest.csv'),
            'backend': 'sql',
        },
        'backends': {
            'sql': {
                'type': 'sql',
                'dsn': sqlite,
            },
        },
        # tests/config/clients/3388ea36-4a4f-4821-900a-b574c8829d52.yml
        'default_auth_client': '3388ea36-4a4f-4821-900a-b574c8829d52',
    })
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
