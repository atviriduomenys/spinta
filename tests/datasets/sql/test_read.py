from spinta.core.config import RawConfig
from spinta.datasets.backends.sql.commands.read import _get_row_value
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
    model = manifest.models['example/City']
    sel = Selected(1, model.properties['rating'])
    assert _get_row_value(context, row, sel) is None


def test_getall_paginate_null_check_value(rc, tmp_path, geodb_null_check):
    create_tabular_manifest(tmp_path / 'manifest.csv', striptable('''
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
