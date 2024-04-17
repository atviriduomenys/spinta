from spinta import commands
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.datasets.backends.sql.commands.read import _get_row_value
from spinta.exceptions import TooShortPageSize
from spinta.manifests.tabular.helpers import striptable
import pytest
from spinta.testing.client import create_client
from spinta.testing.data import listdata
from spinta.testing.datasets import create_sqlite_db
from spinta.testing.manifest import load_manifest_and_context
from spinta.datasets.backends.sql.commands.query import Selected
from spinta.testing.tabular import create_tabular_manifest
import sqlalchemy as sa


@pytest.fixture(scope='module')
def geodb_null_check():
    with create_sqlite_db({
        'cities': [
            sa.Column('id', sa.Integer, primary_key=True),
            sa.Column('name', sa.Text),
        ]
    }) as db:
        db.write('cities', [
            {'id': 0, 'name': 'Vilnius'},
        ])
        yield db


@pytest.fixture(scope='module')
def geodb_with_nulls():
    with create_sqlite_db({
        'cities': [
            sa.Column('id', sa.Integer),
            sa.Column('name', sa.Text),
            sa.Column('code', sa.Text),
            sa.Column('unique', sa.Integer)
        ]
    }) as db:
        db.write('cities', [
            {'id': 0, 'name': 'Vilnius', 'code': 'VLN', 'unique': 0},
            {'id': 0, 'name': 'Vilnius', 'code': 'vln', 'unique': 1},
            {'id': 0, 'name': 'Vilnius', 'code': 'V', 'unique': 2},
            {'id': 0, 'name': None, 'code': None, 'unique': 3},
            {'id': 0, 'name': 'Vilnius', 'code': None, 'unique': 4},
            {'id': 1, 'name': 'Test', 'code': None, 'unique': 5},
            {'id': 2, 'name': None, 'code': None, 'unique': None},
            {'id': 2, 'name': 'Ryga', 'code': 'RG', 'unique': 6},
            {'id': None, 'name': 'EMPTY', 'code': None, 'unique': 7},
            {'id': None, 'name': 'EMPTY', 'code': None, 'unique': 8},
            {'id': None, 'name': 'EMPTY', 'code': None, 'unique': 9},
            {'id': None, 'name': 'EMPTY', 'code': 'ERROR', 'unique': 10},
        ])
        yield db


def test__get_row_value_null(rc: RawConfig):
    context, manifest = load_manifest_and_context(rc, '''
    d | r | b | m | property | type    | ref | source | prepare | access
    example                  |         |     |        |         |
      |   |   | City         |         |     |        |         |
      |   |   |   | name     | string  |     |        |         | open
      |   |   |   | rating   | integer |     |        |         | open
      |   |   |   |          | enum    |     | 1      | 1       |
      |   |   |   |          |         |     | 2      | 2       |
    ''')
    row = ["Vilnius", None]
    model = commands.get_model(context, manifest, 'example/City')
    sel = Selected(1, model.properties['rating'])
    assert _get_row_value(context, row, sel) is None


def test_getall_paginate_null_check_value_sqlite(context, rc, tmp_path, geodb_null_check):
    __test_getall_paginate_null_check_value(context, rc, tmp_path, geodb_null_check)


def __test_getall_paginate_null_check_value(context: Context, rc: RawConfig, tmp_path, db):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
    id | d | r | b | m | property | source          | type    | ref     | access | prepare
       | external/paginate        |                 |         |         |        |
       |   | data                 |                 | sql     |         |        |
       |   |   |                  |                 |         |         |        |
       |   |   |   | City         | cities          |         | id      | open   |
       |   |   |   |   | id       | id              | integer |         |        |
       |   |   |   |   | name     | name            | string  |         |        | 
    '''))

    app = create_client(rc, tmp_path, db)

    resp = app.get('/external/paginate/City')
    assert listdata(resp, 'id', 'name') == [
        (0, 'Vilnius'),
    ]


def test_getall_paginate_with_nulls_page_too_small(context, rc, tmp_path, geodb_with_nulls):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
    id | d | r | b | m | property | source          | type    | ref     | access | prepare
       | external/paginate        |                 |         |         |        |
       |   | data                 |                 | sql     |         |        |
       |   |   |                  |                 |         |         |        |
       |   |   |   | City         | cities          |         | id      | open   |
       |   |   |   |   | id       | id              | integer |         |        |
       |   |   |   |   | name     | name            | string  |         |        | 
       |   |   |   |   | code     | code            | string  |         |        | 
    '''))

    app = create_client(rc, tmp_path, geodb_with_nulls)

    with pytest.raises(BaseException) as e:
        app.get('/external/paginate/City?page(size:2)')
        exceptions = e.exceptions
        assert len(exceptions) == 1
        assert isinstance(exceptions[0], TooShortPageSize)


def test_getall_paginate_with_nulls(context, rc, tmp_path, geodb_with_nulls):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
    id | d | r | b | m | property | source          | type    | ref     | access | prepare
       | external/paginate/null0  |                 |         |         |        |
       |   | data                 |                 | sql     |         |        |
       |   |   |                  |                 |         |         |        |
       |   |   |   | City         | cities          |         | id      | open   |
       |   |   |   |   | id       | id              | integer |         |        |
       |   |   |   |   | name     | name            | string  |         |        | 
       |   |   |   |   | code     | code            | string  |         |        | 
    '''))

    app = create_client(rc, tmp_path, geodb_with_nulls)
    resp = app.get('/external/paginate/null0/City?page(size:6)')
    assert listdata(resp, 'id', 'name', 'code') == [
        (0, 'Vilnius', 'V'),
        (0, 'Vilnius', 'VLN'),
        (0, 'Vilnius', 'vln'),
        (0, 'Vilnius', None),
        (0, None, None),
        (1, 'Test', None),
        (2, 'Ryga', 'RG'),
        (2, None, None),
        (None, 'EMPTY', 'ERROR'),
        (None, 'EMPTY', None),
        (None, 'EMPTY', None),
        (None, 'EMPTY', None),
    ]


def test_getall_paginate_with_nulls_multi_key(context, rc, tmp_path, geodb_with_nulls):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
    id | d | r | b | m | property | source          | type    | ref      | access | prepare
       | external/paginate/null1  |                 |         |          |        |
       |   | data                 |                 | sql     |          |        |
       |   |   |                  |                 |         |          |        |
       |   |   |   | City         | cities          |         | id, code | open   |
       |   |   |   |   | id       | id              | integer |          |        |
       |   |   |   |   | name     | name            | string  |          |        | 
       |   |   |   |   | code     | code            | string  |          |        | 
    '''))

    app = create_client(rc, tmp_path, geodb_with_nulls)
    resp = app.get('/external/paginate/null1/City?page(size:6)')
    assert listdata(resp, 'id', 'name', 'code') == [
        (0, 'Vilnius', 'V'),
        (0, 'Vilnius', 'VLN'),
        (0, 'Vilnius', 'vln'),
        (0, 'Vilnius', None),
        (0, None, None),
        (1, 'Test', None),
        (2, 'Ryga', 'RG'),
        (2, None, None),
        (None, 'EMPTY', 'ERROR'),
        (None, 'EMPTY', None),
        (None, 'EMPTY', None),
        (None, 'EMPTY', None),
    ]


def test_getall_paginate_with_nulls_all_keys(context, rc, tmp_path, geodb_with_nulls):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
    id | d | r | b | m | property | source          | type    | ref      | access | prepare
       | external/paginate/null1  |                 |         |          |        |
       |   | data                 |                 | sql     |          |        |
       |   |   |                  |                 |         |          |        |
       |   |   |   | City         | cities          |         | id, name, code | open   |
       |   |   |   |   | id       | id              | integer |          |        |
       |   |   |   |   | name     | name            | string  |          |        | 
       |   |   |   |   | code     | code            | string  |          |        | 
    '''))

    app = create_client(rc, tmp_path, geodb_with_nulls)
    resp = app.get('/external/paginate/null1/City?page(size:3)')
    assert listdata(resp, 'id', 'name', 'code') == [
        (0, 'Vilnius', 'V'),
        (0, 'Vilnius', 'VLN'),
        (0, 'Vilnius', 'vln'),
        (0, 'Vilnius', None),
        (0, None, None),
        (1, 'Test', None),
        (2, 'Ryga', 'RG'),
        (2, None, None),
        (None, 'EMPTY', 'ERROR'),
        (None, 'EMPTY', None),
        (None, 'EMPTY', None),
        (None, 'EMPTY', None),
    ]


def test_getall_paginate_with_nulls_and_sort(context, rc, tmp_path, geodb_with_nulls):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
    id | d | r | b | m | property | source          | type    | ref      | access | prepare
       | external/paginate/null2        |                 |         |          |        |
       |   | data                 |                 | sql     |          |        |
       |   |   |                  |                 |         |          |        |
       |   |   |   | City         | cities          |         | id       | open   |
       |   |   |   |   | id       | id              | integer |          |        |
       |   |   |   |   | name     | name            | string  |          |        | 
       |   |   |   |   | code     | code            | string  |          |        | 
    '''))

    app = create_client(rc, tmp_path, geodb_with_nulls)
    resp = app.get('/external/paginate/null2/City?sort(name)&page(size:6)')
    assert listdata(resp, 'id', 'name', 'code') == [
        (0, 'Vilnius', 'V'),
        (0, 'Vilnius', 'VLN'),
        (0, 'Vilnius', 'vln'),
        (0, 'Vilnius', None),
        (0, None, None),
        (1, 'Test', None),
        (2, 'Ryga', 'RG'),
        (2, None, None),
        (None, 'EMPTY', 'ERROR'),
        (None, 'EMPTY', None),
        (None, 'EMPTY', None),
        (None, 'EMPTY', None),
    ]


def test_getall_paginate_with_nulls_unique(context, rc, tmp_path, geodb_with_nulls):
    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
    id | d | r | b | m | property | source          | type    | ref      | access | prepare
       | external/paginate/null3        |                 |         |          |        |
       |   | data                 |                 | sql     |          |        |
       |   |   |                  |                 |         |          |        |
       |   |   |   | City         | cities          |         | name, unique | open   |
       |   |   |   |   | id       | id              | integer |          |        |
       |   |   |   |   | name     | name            | string  |          |        | 
       |   |   |   |   | code     | code            | string  |          |        | 
       |   |   |   |   | unique   | unique          | integer |          |        | 
    '''))

    app = create_client(rc, tmp_path, geodb_with_nulls)
    resp = app.get('/external/paginate/null3/City?sort(name, -unique)&page(size:1)')
    assert listdata(resp, 'name', 'unique', 'id', 'code') == [
        ('EMPTY', 10, None, 'ERROR'),
        ('EMPTY', 7, None, None),
        ('EMPTY', 8, None, None),
        ('EMPTY', 9, None, None),
        ('Ryga', 6, 2, 'RG'),
        ('Test', 5, 1, None),
        ('Vilnius', 0, 0, 'VLN'),
        ('Vilnius', 1, 0, 'vln'),
        ('Vilnius', 2, 0, 'V'),
        ('Vilnius', 4, 0, None),
        (None, 3, 0, None),
        (None, None, 2, None),
    ]

    resp = app.get('/external/paginate/null3/City?sort(name, unique)&page(size:1)')
    assert listdata(resp, 'name', 'unique', 'id', 'code') == [
        ('EMPTY', 10, None, 'ERROR'),
        ('EMPTY', 7, None, None),
        ('EMPTY', 8, None, None),
        ('EMPTY', 9, None, None),
        ('Ryga', 6, 2, 'RG'),
        ('Test', 5, 1, None),
        ('Vilnius', 0, 0, 'VLN'),
        ('Vilnius', 1, 0, 'vln'),
        ('Vilnius', 2, 0, 'V'),
        ('Vilnius', 4, 0, None),
        (None, 3, 0, None),
        (None, None, 2, None),
    ]


def test_getall_distinct(context, rc, tmp_path):
    with create_sqlite_db({
        'cities': [
            sa.Column('name', sa.Text),
            sa.Column('country', sa.Text),
            sa.Column('id', sa.Integer)
        ]
    }) as db:
        db.write('cities', [
            {'name': 'Vilnius', 'country': 'Lietuva', 'id': 0},
            {'name': 'Kaunas', 'country': 'Lietuva', 'id': 0},
            {'name': 'Siauliai', 'country': 'Lietuva', 'id': 1},
        ])
        create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
        id | d | r | b | m | property    | source          | type    | ref      | access | prepare    | level
           | external/distinct           |                 |         |          |        |            |
           |   | data                    |                 | sql     |          |        |            |
           |   |   |                     |                 |         |          |        |            |
           |   |   |   | City            | cities          |         | name     | open   |            |
           |   |   |   |   | name        | name            | string  |          |        |            |
           |   |   |   |   | country     | country         | ref     | Country  |        |            | 3
           |   |   |   | Country         | cities          |         | name     | open   |            |
           |   |   |   |   | name        | country         | string  |          |        |            |
           |   |   |   |   | id          | id              | integer |          |        |            |     
           |   |   |   | CountryDistinct | cities          |         | name     | open   | distinct() |
           |   |   |   |   | name        | country         | string  |          |        |            |
           |   |   |   |   | id          | id              | integer |          |        |            |        
           |   |   |   | CountryMultiDistinct | cities          |         | name, id | open   | distinct() |
           |   |   |   |   | name        | country         | string  |          |        |            |
           |   |   |   |   | id          | id              | integer |          |        |            |       
           |   |   |   | CountryAllDistinct | cities          |         |        | open   | distinct() |
           |   |   |   |   | name        | country         | string  |          |        |            |
           |   |   |   |   | id          | id              | integer |          |        |            |   
        '''))

        app = create_client(rc, tmp_path, db)
        resp = app.get('/external/distinct/City')
        assert listdata(resp, 'name', 'country') == [
            ('Kaunas', {'name': 'Lietuva'}),
            ('Siauliai', {'name': 'Lietuva'}),
            ('Vilnius', {'name': 'Lietuva'})
        ]

        resp = app.get('/external/distinct/Country')
        assert listdata(resp, 'name', 'id') == [
            ('Lietuva', 0),
            ('Lietuva', 0),
            ('Lietuva', 1)
        ]

        resp = app.get('/external/distinct/CountryDistinct')
        assert listdata(resp, 'name', 'id') == [
            ('Lietuva', 0),
        ]

        resp = app.get('/external/distinct/CountryMultiDistinct')
        assert listdata(resp, 'name', 'id') == [
            ('Lietuva', 0),
            ('Lietuva', 1)
        ]

        resp = app.get('/external/distinct/CountryAllDistinct')
        assert listdata(resp, 'name', 'id') == [
            ('Lietuva', 0),
            ('Lietuva', 1)
        ]
