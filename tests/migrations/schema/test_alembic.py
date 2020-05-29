import pytest

import sqlalchemy as sa

from alembic.runtime.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.dialects.postgresql import UUID

from spinta.backends.postgresql.components import PostgreSQL
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
        'backend': PostgreSQL()
    })


def test_create_table(engine, ufunc, request):
    ufunc('''\
    create_table(
        '_test_table_country',
        column(_id, uuid(), primary_key: true),
        column(_revision, string(), unique: true),
        column(name, string(), nullable: true),
    )
    ''')
    ufunc('''
    create_table(
        '_test_table_city',
        column(_id, uuid(), primary_key: true),
        column(_revision, string(), unique: true),
        column(name, string(), nullable: true),
        column('country._id', uuid(), ref('_test_table_country._id', ondelete: 'CASCADE')),
        column('country', json()),
        column('flags', array(uuid())),
    )
    ''')
    meta = sa.MetaData(engine)
    request.addfinalizer(meta.drop_all)
    country = sa.Table('_test_table_country', meta, autoload=True)
    city = sa.Table('_test_table_city', meta, autoload=True)
    assert country.primary_key.columns.keys() == ['_id']
    assert city.columns.keys() == ['_id', '_revision', 'name', 'country._id', 'country', 'flags']
    assert next(iter(city.c['country._id'].foreign_keys)).ondelete == 'CASCADE'


def test_drop_table(engine, ufunc):
    meta = sa.MetaData(engine)
    table = sa.Table('_test_table', meta, sa.Column('id', sa.String()))
    table.create()
    ufunc('drop_table("_test_table")')
    assert not table.exists()


def test_add_column(engine, ufunc, request):
    meta = sa.MetaData(engine)
    table = sa.Table('_test_add_colunm_table', meta, sa.Column('id', sa.String()))
    table.create()
    ufunc('''
    add_column(
        '_test_add_colunm_table',
        column('new_column', pk())
    )
    ''')

    meta = sa.MetaData(engine)
    request.addfinalizer(meta.drop_all)
    table = sa.Table('_test_add_colunm_table', meta, autoload=True)
    assert table.columns.keys() == ['id', 'new_column']
    type_ = [i.type for i in table.columns.values() if i.name == 'new_column'][0]
    assert isinstance(type_, UUID)


def test_drop_column(engine, ufunc, request):
    meta = sa.MetaData(engine)
    table = sa.Table(
        '_test_drop_column_table',
        meta,
        sa.Column('id', sa.String()),
        sa.Column('name', sa.String())
    )
    table.create()
    ufunc('drop_column("_test_drop_column_table", "name")')

    meta = sa.MetaData(engine)
    request.addfinalizer(meta.drop_all)
    table = sa.Table('_test_drop_column_table', meta, autoload=True)
    assert table.columns.keys() == ['id']


def test_alter_column(engine, ufunc, request):
    meta = sa.MetaData(engine)
    table = sa.Table(
        '_test_alter_column_table',
        meta,
        sa.Column('id', sa.String()),
    )
    table.create()
    ufunc('''
    alter_column(
        '_test_alter_column_table',
        'id',
        nullable: true
    )
    ''')

    meta = sa.MetaData(engine)
    request.addfinalizer(meta.drop_all)
    table = sa.Table('_test_alter_column_table', meta, autoload=True)
    assert table.columns.values()[0].nullable is True
