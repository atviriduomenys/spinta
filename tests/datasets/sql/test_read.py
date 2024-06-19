from spinta import commands
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.datasets.backends.sql.components import Sql
from spinta.exceptions import TooShortPageSize
from spinta.manifests.tabular.helpers import striptable
import pytest
from spinta.testing.client import create_client
from spinta.testing.data import listdata
from spinta.testing.datasets import create_sqlite_db, use_default_dialect_functions
from spinta.testing.manifest import load_manifest_and_context
from spinta.testing.tabular import create_tabular_manifest
import sqlalchemy as sa

from spinta.ufuncs.basequerybuilder.components import Selected
from spinta.ufuncs.resultbuilder.helpers import get_row_value


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
    assert get_row_value(context, Sql(), row, sel) is None


@pytest.mark.parametrize("use_default_dialect", [True, False])
def test_getall_paginate_invalid_type(use_default_dialect: bool, context: Context, rc: RawConfig, tmp_path, geodb_null_check, mocker):
    if use_default_dialect:
        use_default_dialect_functions(mocker)

    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
    id | d | r | b | m | property | source          | type    | ref      | access | prepare
       | external/paginate        |                 |         |          |        |
       |   | data                 |                 | sql     |          |        |
       |   |   |                  |                 |         |          |        |
       |   |   |   | City         | cities          |         | id, test | open   |
       |   |   |   |   | id       | id              | integer |          |        |
       |   |   |   |   | name     | name            | string  |          |        | 
       |   |   |   |   | test     | name            | object  |          |        | 
    '''))

    app = create_client(rc, tmp_path, geodb_null_check)

    resp = app.get('/external/paginate/City')
    assert listdata(resp, 'id', 'name') == [
        (0, 'Vilnius'),
    ]
    assert '_page' not in resp.json()


@pytest.mark.parametrize("use_default_dialect", [True, False])
def test_getall_paginate_null_check_value(use_default_dialect: bool, context: Context, rc: RawConfig, tmp_path, geodb_null_check, mocker):
    if use_default_dialect:
        use_default_dialect_functions(mocker)

    create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable('''
    id | d | r | b | m | property | source          | type    | ref     | access | prepare
       | external/paginate        |                 |         |         |        |
       |   | data                 |                 | sql     |         |        |
       |   |   |                  |                 |         |         |        |
       |   |   |   | City         | cities          |         | id      | open   |
       |   |   |   |   | id       | id              | integer |         |        |
       |   |   |   |   | name     | name            | string  |         |        | 
    '''))

    app = create_client(rc, tmp_path, geodb_null_check)

    resp = app.get('/external/paginate/City')
    assert listdata(resp, 'id', 'name') == [
        (0, 'Vilnius'),
    ]


@pytest.mark.parametrize("use_default_dialect", [True, False])
def test_getall_paginate_with_nulls_page_too_small(use_default_dialect: bool, context: Context, rc: RawConfig, tmp_path, geodb_with_nulls, mocker):
    if use_default_dialect:
        use_default_dialect_functions(mocker)

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


@pytest.mark.parametrize("use_default_dialect", [True, False])
def test_getall_paginate_with_nulls(use_default_dialect: bool, context: Context, rc: RawConfig, tmp_path, geodb_with_nulls, mocker):
    if use_default_dialect:
        use_default_dialect_functions(mocker)

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


@pytest.mark.parametrize("use_default_dialect", [True, False])
def test_getall_paginate_with_nulls_multi_key(use_default_dialect: bool, context: Context, rc: RawConfig, tmp_path, geodb_with_nulls, mocker):
    if use_default_dialect:
        use_default_dialect_functions(mocker)

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


@pytest.mark.parametrize("use_default_dialect", [True, False])
def test_getall_paginate_with_nulls_all_keys(use_default_dialect: bool, context: Context, rc: RawConfig, tmp_path, geodb_with_nulls, mocker):
    if use_default_dialect:
        use_default_dialect_functions(mocker)

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


@pytest.mark.parametrize("use_default_dialect", [True, False])
def test_getall_paginate_with_nulls_and_sort(use_default_dialect: bool, context: Context, rc: RawConfig, tmp_path, geodb_with_nulls, mocker):
    if use_default_dialect:
        use_default_dialect_functions(mocker)

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


@pytest.mark.parametrize("use_default_dialect", [True, False])
def test_getall_paginate_with_nulls_unique(use_default_dialect: bool, context: Context, rc: RawConfig, tmp_path, geodb_with_nulls, mocker):
    if use_default_dialect:
        use_default_dialect_functions(mocker)

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


def test_get_one(context, rc, tmp_path):

    with create_sqlite_db({
        'cities': [
            sa.Column('name', sa.Text),
            sa.Column('id', sa.Integer)
        ]
    }) as db:
        db.write('cities', [
            {'name': 'Vilnius', 'id': 0},
            {'name': 'Kaunas', 'id': 1},
        ])
        create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable(f'''
id | d | r | b | m | property     | type    | ref | level | source  | access
   | example                      |         |     |       |         |
   |   | db                       | sql     |     |       |         |
   |   |   |   | City             |         | id  |       | cities  |
   |   |   |   |   | id           | integer |     | 4     | id      | open
   |   |   |   |   | name         | string  |     | 4     | name    | open
  '''))
        app = create_client(rc, tmp_path, db)
        response = app.get('/example/City')
        response_json = response.json()
        print(response_json)
        _id = response_json["_data"][0]["_id"]
        getone_response = app.get(f'/example/City/{_id}')
        result = getone_response.json()
        assert result == {
            "_id": _id,
            "_type": "example/City",
            "id": 0,
            "name": "Vilnius"
        }


def test_get_one_compound_pk(context, rc, tmp_path):

    with create_sqlite_db({
        'cities': [
            sa.Column('name', sa.Text),
            sa.Column('id', sa.Integer),
            sa.Column('code', sa.Integer)
        ]
    }) as db:
        db.write('cities', [
            {'name': 'Vilnius', 'id': 4, "code": "city"},
            {'name': 'Kaunas', 'id': 2, "code": "city"},
        ])
        create_tabular_manifest(context, tmp_path / 'manifest.csv', striptable(f'''
id | d | r | b | m | property     | type    | ref | level | source  | access
   | example                      |         |     |       |         |
   |   | db                       | sql     |     |       |         |
   |   |   |   | City             |         | id, code  |       | cities  |
   |   |   |   |   | id           | integer |     | 4     | id      | open
   |   |   |   |   | name         | string  |     | 4     | name    | open
   |   |   |   |   | code         | string  |     | 4     | code    | open
  '''))
        app = create_client(rc, tmp_path, db)
        response = app.get('/example/City')
        response_json = response.json()
        _id = response_json["_data"][0]["_id"]
        getone_response = app.get(f'/example/City/{_id}')
        result = getone_response.json()
        assert result == {
            "_id": _id,
            "_type": "example/City",
            "code": "city",
            "id": 2,
            "name": "Kaunas"
        }
