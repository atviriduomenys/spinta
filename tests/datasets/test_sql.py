import pathlib
import os
import tempfile

import pytest

import sqlalchemy as sa

from spinta.core.config import RawConfig
from spinta.testing.client import create_test_client
from spinta.testing.context import create_test_context
from spinta.testing.datasets import striptable
from spinta.testing.datasets import create_tabular_manifest


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
            'backend': 'default',
        },
        'backends': {
            'default': {
                'type': 'memory',
            },
            'sql': {
                'type': 'sql',
                'dsn': sqlite,
            },
        },
    })
    context = create_test_context(rc)
    return create_test_client(context)


def query(app, qry, *cols):
    resp = app.get(qry)
    data = resp.json()
    assert resp.status_code == 200, data
    assert '_data' in data, data
    return [
        tuple(d[c] for c in cols)
        for d in data['_data']
    ]


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

    app.authmodel('datasets/gov/example/country', ['getall_external'])
    qry = '/datasets/gov/example/country/:external'
    assert query(app, qry, 'code', 'name') == [
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

    app.authmodel('datasets/gov/example/city', ['search_external'])
    qry = '/datasets/gov/example/city/:external?sort(name)'
    assert query(app, qry, 'country', 'name') == [
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

    app.authmodel('datasets/gov/example/city', ['search_external'])
    qry = '/datasets/gov/example/city/:external?sort(name)'
    assert query(app, qry, 'country', 'name') == [
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

    app.authmodel('datasets/gov/example/city', ['search_external'])
    qry = '/datasets/gov/example/city/:external?sort(name)'
    assert query(app, qry, 'country', 'name') == [
        ({'_id': 'ee'}, 'Talinas'),
    ]


@pytest.mark.skip('TODO')
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

    app.authmodel('datasets/gov/example/city', ['search_external'])
    qry = '/datasets/gov/example/city/:external?sort(name)'
    assert query(app, qry, 'country', 'name') == [
        ({'_id': 'ee'}, 'Talinas'),
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

    app.authmodel('datasets/gov/example/country', ['search_external'])
    qry = '/datasets/gov/example/country/:external?sort(code)'
    assert query(app, qry, 'code', 'name', '_type') == [
        ('ee', 'Estija', 'datasets/gov/example/country'),
        ('lt', 'Lietuva', 'datasets/gov/example/country'),
        ('lv', 'Latvija', 'datasets/gov/example/country'),
    ]

    app.authmodel('datasets/gov/example/city', ['search_external'])
    qry = '/datasets/gov/example/city/:external?sort(name)'
    assert query(app, qry, 'country', 'name', '_type') == [
        ({'_id': 'lv'}, 'Ryga', 'datasets/gov/example/city'),
        ({'_id': 'ee'}, 'Talinas', 'datasets/gov/example/city'),
        ({'_id': 'lt'}, 'Vilnius', 'datasets/gov/example/city'),
    ]
