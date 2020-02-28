import pytest

import sqlalchemy as sa

from alembic.migration import MigrationContext
from alembic.operations import Operations

from spinta.testing.ufuncs import UFuncTester
from spinta.migrations.schema.alembic import Alembic


@pytest.fixture(scope='module')
def engine(postgresql):
    return sa.create_engine(postgresql)


@pytest.fixture()
def ufunc(context, engine):
    conn = engine.connect()
    ctx = MigrationContext.configure(conn)
    op = Operations(ctx)
    return UFuncTester(Alembic, context, scope={
        'op': op,
    })


def test_create_table(engine, ufunc, request):
    ufunc('''\
    create_table(
        '_test_table',
        column(_id, uuid(), primary_key: true),
        column(_revision, string(), unique: true),
        column(name, string(), nullable: true),
        column('foo.bar', string(), nullable: true),
    )
    ''')
    meta = sa.MetaData(engine)
    request.addfinalizer(meta.drop_all)
    table = sa.Table('_test_table', meta, autoload=True)
    assert table.primary_key.columns.keys() == ['_id']
    assert table.columns.keys() == ['_id', '_revision', 'name', 'foo.bar']


def test_drop_table(engine, ufunc):
    meta = sa.MetaData(engine)
    table = sa.Table('_test_table', meta, sa.Column('id', sa.String()))
    table.create()
    ufunc('drop_table("_test_table")')
    assert not table.exists()
