import itertools
import json
from pathlib import Path
from pprint import pprint

import pytest

from spinta.backends.postgresql.helpers import get_pg_name
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.context import create_test_context
from spinta.testing.tabular import create_tabular_manifest
from geoalchemy2.shape import to_shape

import sqlalchemy as sa
from sqlalchemy.engine.url import make_url, URL
import sqlalchemy_utils as su

MIGRATION_DATABASE = "spinta_tests_migration"


@pytest.fixture(scope='module')
def postgresql_migration(rc) -> URL:
    url = make_url(rc.get('backends', 'default', 'dsn', required=True))
    url = url.set(database=MIGRATION_DATABASE)

    if su.database_exists(url):
        _prepare_migration_postgresql(url)
        yield url
    else:
        su.create_database(url)
        _prepare_migration_postgresql(url)
        yield url
        su.drop_database(url)


def _prepare_migration_postgresql(dsn: URL) -> None:
    engine = sa.create_engine(dsn)
    with engine.connect() as conn:
        conn.execute(sa.text("DROP SCHEMA public CASCADE"))
        conn.execute(sa.text("CREATE SCHEMA public"))
        conn.execute(sa.text("CREATE EXTENSION IF NOT EXISTS postgis"))
        conn.execute(sa.text("CREATE EXTENSION IF NOT EXISTS postgis_topology"))
        conn.execute(sa.text("CREATE EXTENSION IF NOT EXISTS fuzzystrmatch"))
        conn.execute(sa.text("CREATE EXTENSION IF NOT EXISTS postgis_tiger_geocoder"))


def _configure(rc, path, manifest):
    url = make_url(rc.get('backends', 'default', 'dsn', required=True))
    url = url.set(database=MIGRATION_DATABASE)
    rc = rc.fork({
        'manifests': {
            'default': {
                'type': 'tabular',
                'path': str(path),
                'backend': 'default',
                'keymap': 'default',
                'mode': 'external',
            },
        },
        'backends': {
            'default': {
                'type': 'postgresql',
                'dsn': url
            },
        },
    })
    context = create_test_context(rc, name='pytest/cli')
    override_manifest(context, path, manifest)
    return context, rc


def override_manifest(context: Context, path, manifest):
    path = f'{path}/manifest.csv'
    create_tabular_manifest(context, path, striptable(manifest))


def _clean_up_tables(meta: sa.MetaData, tables: list):
    table_list = []
    for table in tables:
        table_list.append(meta.tables[get_pg_name(table)])
    meta.drop_all(tables=table_list)


def float_equals(a: float, b: float, epsilon=1e-9):
    return abs(a - b) < epsilon


def _get_table_unique_constraint_columns(table: sa.Table):
    constraint_columns = []
    for constraint in table.constraints:
        if isinstance(constraint, sa.UniqueConstraint):
            column_names = [column.name for column in constraint.columns]
            constraint_columns.append(column_names)
    return constraint_columns


def _get_table_foreign_key_constraint_columns(table: sa.Table):
    constraint_columns = []
    for constraint in table.constraints:
        if isinstance(constraint, sa.ForeignKeyConstraint):
            column_names = [column.name for column in constraint.columns]
            element_names = [element.column.name for element in constraint.elements]
            constraint_columns.append({
                'constraint_name': constraint.name,
                'column_names': column_names,
                'referred_column_names': element_names
            })
    return constraint_columns


def cleanup_tables(postgresql_migration: URL):
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        drop_tables = []
        for table in tables.values():
            if not table.name.startswith("_"):
                if table.name != "spatial_ref_sys":
                    drop_tables.append(table)
        meta.drop_all(tables=drop_tables)


def test_migrate_create_simple_datatype_model(
    postgresql_migration: URL,
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = '''
     d | r | b | m | property   | type    | ref     | source     | prepare
    '''
    context, rc = _configure(rc, tmp_path, initial_manifest)
    cli.invoke(rc, [
        'bootstrap', f'{tmp_path}/manifest.csv'
    ])
    override_manifest(context, tmp_path, '''
     d               | r | b | m    | property     | type
     migrate/example |   |   |      |              |
                     |   |   | Test |              |
                     |   |   |      | someText     | string
                     |   |   |      | someInteger  | integer
                     |   |   |      | someNumber   | number
                     |   |   |      | someDate     | date
                     |   |   |      | someDateTime | datetime
                     |   |   |      | someTime     | time
                     |   |   |      | someBoolean  | boolean
                     |   |   |      | someUrl      | url
                     |   |   |      | someUri      | uri
                     |   |   |      | someBinary   | binary
                     |   |   |      | someJson     | json
    ''')
    result = cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv', '-p'
    ])
    assert result.output.endswith(
        'BEGIN;\n'
        '\n'
        'CREATE TABLE "migrate/example/Test" (\n'
        '    _txn UUID, \n'
        '    _created TIMESTAMP WITHOUT TIME ZONE, \n'
        '    _updated TIMESTAMP WITHOUT TIME ZONE, \n'
        '    _id UUID NOT NULL, \n'
        '    _revision TEXT, \n'
        '    "someText" TEXT, \n'
        '    "someInteger" INTEGER, \n'
        '    "someNumber" FLOAT, \n'
        '    "someDate" DATE, \n'
        '    "someDateTime" TIMESTAMP WITHOUT TIME ZONE, \n'
        '    "someTime" TIME WITHOUT TIME ZONE, \n'
        '    "someBoolean" BOOLEAN, \n'
        '    "someUrl" VARCHAR, \n'
        '    "someUri" VARCHAR, \n'
        '    "someBinary" BYTEA, \n'
        '    "someJson" JSONB, \n'
        '    PRIMARY KEY (_id), \n'
        '    UNIQUE ("someUri")\n'
        ');\n'
        '\n'
        'CREATE INDEX "ix_migrate/example/Test__txn" ON "migrate/example/Test" '
        '(_txn);\n'
        '\n'
        'CREATE TABLE "migrate/example/Test/:changelog" (\n'
        '    _id BIGSERIAL NOT NULL, \n'
        '    _revision VARCHAR, \n'
        '    _txn UUID, \n'
        '    _rid UUID, \n'
        '    datetime TIMESTAMP WITHOUT TIME ZONE, \n'
        '    action VARCHAR(8), \n'
        '    data JSONB, \n'
        '    PRIMARY KEY (_id)\n'
        ');\n'
        '\n'
        'CREATE INDEX "ix_migrate/example/Test/:changelog__txn" ON '
        '"migrate/example/Test/:changelog" (_txn);\n'
        '\n'
        'COMMIT;\n'
        '\n')
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert not {'migrate/example/Test', 'migrate/example/Test/:changelog'}.issubset(tables.keys())
    cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv'
    ])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog'}.issubset(tables.keys())

        columns = tables['migrate/example/Test'].columns
        assert {
            'someText',
            'someInteger',
            'someNumber',
            'someDate',
            'someDateTime',
            'someTime',
            'someBoolean',
            'someUrl',
            'someUri',
            'someBinary',
            'someJson'
        }.issubset(columns.keys())

        some_text = columns['someText']
        assert isinstance(some_text.type, sa.String)
        assert some_text.nullable

        some_integer = columns['someInteger']
        assert isinstance(some_integer.type, sa.Integer)
        assert some_integer.nullable

        some_number = columns['someNumber']
        assert isinstance(some_number.type, sa.Float)
        assert some_number.nullable

        some_date = columns['someDate']
        assert isinstance(some_date.type, sa.Date)
        assert some_date.nullable

        some_date_time = columns['someDateTime']
        assert isinstance(some_date_time.type, sa.DateTime)
        assert some_date_time.nullable

        some_time = columns['someTime']
        assert isinstance(some_time.type, sa.Time)
        assert some_time.nullable

        some_boolean = columns['someBoolean']
        assert isinstance(some_boolean.type, sa.Boolean)
        assert some_boolean.nullable

        some_url = columns['someUrl']
        assert isinstance(some_url.type, sa.String)
        assert some_url.nullable

        some_uri = columns['someUri']
        assert isinstance(some_uri.type, sa.String)
        assert some_uri.nullable

        some_binary = columns['someBinary']
        assert isinstance(some_binary.type, sa.LargeBinary)
        assert some_binary.nullable

        some_json = columns['someJson']
        assert isinstance(some_json.type, sa.JSON)
        assert some_json.nullable

        _clean_up_tables(meta, ['migrate/example/Test', 'migrate/example/Test/:changelog'])


def test_migrate_add_simple_column(
    postgresql_migration: URL,
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = '''
     d               | r | b | m    | property     | type
     migrate/example |   |   |      |              |
                     |   |   | Test |              |
                     |   |   |      | someText     | string
    '''
    context, rc = _configure(rc, tmp_path, initial_manifest)

    cli.invoke(rc, [
        'bootstrap', f'{tmp_path}/manifest.csv'
    ])

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog'}.issubset(tables.keys())
        columns = tables['migrate/example/Test'].columns
        assert {'someText'}.issubset(columns.keys())

        some_text = columns['someText']
        assert isinstance(some_text.type, sa.String)
        assert some_text.nullable

        assert not {'someInteger'}.issubset(columns.keys())

    override_manifest(context, tmp_path, '''
     d               | r | b | m    | property     | type
     migrate/example |   |   |      |              |
                     |   |   | Test |              |
                     |   |   |      | someText     | string
                     |   |   |      | someInteger  | integer
    ''')

    result = cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv', '-p'
    ])
    assert result.output.endswith(
        'BEGIN;\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" ADD COLUMN "someInteger" INTEGER;\n'
        '\n'
        'COMMIT;\n'
        '\n')

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog'}.issubset(tables.keys())

        columns = tables['migrate/example/Test'].columns
        assert {'someText'}.issubset(columns.keys())

        assert not {'someInteger'}.issubset(columns.keys())

    cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv'
    ])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog'}.issubset(tables.keys())

        columns = tables['migrate/example/Test'].columns
        assert {'someText', 'someInteger'}.issubset(columns.keys())

        some_integer = columns['someInteger']
        assert isinstance(some_integer.type, sa.Integer)
        assert some_integer.nullable

        _clean_up_tables(meta, ['migrate/example/Test', 'migrate/example/Test/:changelog'])


def test_migrate_remove_simple_column(
    postgresql_migration: URL,
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = '''
     d               | r | b | m    | property     | type
     migrate/example |   |   |      |              |
                     |   |   | Test |              |
                     |   |   |      | someText     | string
                     |   |   |      | someInteger  | integer
    '''
    context, rc = _configure(rc, tmp_path, initial_manifest)

    cli.invoke(rc, [
        'bootstrap', f'{tmp_path}/manifest.csv'
    ])

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog'}.issubset(tables.keys())
        columns = tables['migrate/example/Test'].columns
        assert {'someText', 'someInteger'}.issubset(columns.keys())

        some_text = columns['someText']
        assert isinstance(some_text.type, sa.String)
        assert some_text.nullable

        some_integer = columns['someInteger']
        assert isinstance(some_integer.type, sa.Integer)
        assert some_integer.nullable

    override_manifest(context, tmp_path, '''
     d               | r | b | m    | property     | type
     migrate/example |   |   |      |              |
                     |   |   | Test |              |
                     |   |   |      | someText     | string
    ''')

    result = cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv', '-p'
    ])
    assert result.output.endswith(
        'BEGIN;\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" RENAME "someInteger" TO '
        '"__someInteger";\n'
        '\n'
        'COMMIT;\n'
        '\n')

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog'}.issubset(tables.keys())

        columns = tables['migrate/example/Test'].columns
        assert {'someText', 'someInteger'}.issubset(columns.keys())

        some_integer = columns['someInteger']
        assert isinstance(some_integer.type, sa.Integer)
        assert some_integer.nullable

        assert not {'__someInteger'}.issubset(columns.keys())

    cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv'
    ])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog'}.issubset(tables.keys())

        columns = tables['migrate/example/Test'].columns
        assert {'someText', '__someInteger'}.issubset(columns.keys())

        some_integer = columns['__someInteger']
        assert isinstance(some_integer.type, sa.Integer)
        assert some_integer.nullable

        assert not {'someInteger'}.issubset(columns.keys())
        _clean_up_tables(meta, ['migrate/example/Test', 'migrate/example/Test/:changelog'])


def test_migrate_multiple_times_remove_simple_column(
    postgresql_migration: URL,
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = '''
     d               | r | b | m    | property     | type
     migrate/example |   |   |      |              |
                     |   |   | Test |              |
                     |   |   |      | someText     | string
                     |   |   |      | someInteger  | integer
    '''
    context, rc = _configure(rc, tmp_path, initial_manifest)

    cli.invoke(rc, [
        'bootstrap', f'{tmp_path}/manifest.csv'
    ])

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog'}.issubset(tables.keys())
        columns = tables['migrate/example/Test'].columns
        assert {'someText', 'someInteger'}.issubset(columns.keys())

        some_text = columns['someText']
        assert isinstance(some_text.type, sa.String)
        assert some_text.nullable

        some_integer = columns['someInteger']
        assert isinstance(some_integer.type, sa.Integer)
        assert some_integer.nullable

    override_manifest(context, tmp_path, '''
     d               | r | b | m    | property     | type
     migrate/example |   |   |      |              |
                     |   |   | Test |              |
                     |   |   |      | someText     | string
    ''')

    result = cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv', '-p'
    ])
    assert result.output.endswith(
        'BEGIN;\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" RENAME "someInteger" TO '
        '"__someInteger";\n'
        '\n'
        'COMMIT;\n'
        '\n')

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog'}.issubset(tables.keys())

        columns = tables['migrate/example/Test'].columns
        assert {'someText', 'someInteger'}.issubset(columns.keys())

        assert not {'__someInteger'}.issubset(columns.keys())

    cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv'
    ])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog'}.issubset(tables.keys())

        columns = tables['migrate/example/Test'].columns
        assert {'someText', '__someInteger'}.issubset(columns.keys())

        some_integer = columns['__someInteger']
        assert isinstance(some_integer.type, sa.Integer)
        assert some_integer.nullable

        assert not {'someInteger'}.issubset(columns.keys())

    override_manifest(context, tmp_path, '''
         d               | r | b | m    | property     | type
         migrate/example |   |   |      |              |
                         |   |   | Test |              |
                         |   |   |      | someText     | string
                         |   |   |      | someInteger  | integer
        ''')
    cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv'
    ])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog'}.issubset(tables.keys())

        columns = tables['migrate/example/Test'].columns
        assert {'someText', 'someInteger', '__someInteger'}.issubset(columns.keys())

        some_integer = columns['someInteger']
        assert isinstance(some_integer.type, sa.Integer)
        assert some_integer.nullable

    override_manifest(context, tmp_path, '''
         d               | r | b | m    | property     | type
         migrate/example |   |   |      |              |
                         |   |   | Test |              |
                         |   |   |      | someText     | string
        ''')

    result = cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv', '-p'
    ])
    assert result.output.endswith(
        'BEGIN;\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" DROP COLUMN "__someInteger";\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" RENAME "someInteger" TO '
        '"__someInteger";\n'
        '\n'
        'COMMIT;\n'
        '\n')
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog'}.issubset(tables.keys())

        columns = tables['migrate/example/Test'].columns
        assert {'someText', 'someInteger', '__someInteger'}.issubset(columns.keys())

    cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv'
    ])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog'}.issubset(tables.keys())

        columns = tables['migrate/example/Test'].columns
        assert {'someText', '__someInteger'}.issubset(columns.keys())

        some_integer = columns['__someInteger']
        assert isinstance(some_integer.type, sa.Integer)
        assert some_integer.nullable

        assert not {'someInteger'}.issubset(columns.keys())

        _clean_up_tables(meta, ['migrate/example/Test', 'migrate/example/Test/:changelog'])


def test_migrate_model_ref_unique_constraint(
    postgresql_migration: URL,
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = '''
     d               | r | b | m    | property     | type   | ref
    '''
    context, rc = _configure(rc, tmp_path, initial_manifest)

    cli.invoke(rc, [
        'bootstrap', f'{tmp_path}/manifest.csv'
    ])

    override_manifest(context, tmp_path, '''
     d               | r | b | m     | property     | type          | ref
     migrate/example |   |   |       |              |               |
                     |   |   | Test  |              |               | someText
                     |   |   |       | someText     | string        |
                     |   |   |       |              |               |
                     |   |   | Multi |              |               | someText, someNumber
                     |   |   |       | someText     | string        |
                     |   |   |       | someInteger  | integer       |
                     |   |   |       | someNumber   | number unique |
    ''')

    result = cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv', '-p'
    ])
    assert result.output.endswith(
        'BEGIN;\n'
        '\n'
        'CREATE TABLE "migrate/example/Test" (\n'
        '    _txn UUID, \n'
        '    _created TIMESTAMP WITHOUT TIME ZONE, \n'
        '    _updated TIMESTAMP WITHOUT TIME ZONE, \n'
        '    _id UUID NOT NULL, \n'
        '    _revision TEXT, \n'
        '    "someText" TEXT, \n'
        '    PRIMARY KEY (_id)\n'
        ');\n'
        '\n'
        'CREATE INDEX "ix_migrate/example/Test__txn" ON "migrate/example/Test" '
        '(_txn);\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" ADD CONSTRAINT '
        '"migrate/example/Test_someText_key" UNIQUE ("someText");\n'
        '\n'
        'CREATE TABLE "migrate/example/Test/:changelog" (\n'
        '    _id BIGSERIAL NOT NULL, \n'
        '    _revision VARCHAR, \n'
        '    _txn UUID, \n'
        '    _rid UUID, \n'
        '    datetime TIMESTAMP WITHOUT TIME ZONE, \n'
        '    action VARCHAR(8), \n'
        '    data JSONB, \n'
        '    PRIMARY KEY (_id)\n'
        ');\n'
        '\n'
        'CREATE INDEX "ix_migrate/example/Test/:changelog__txn" ON '
        '"migrate/example/Test/:changelog" (_txn);\n'
        '\n'
        'CREATE TABLE "migrate/example/Multi" (\n'
        '    _txn UUID, \n'
        '    _created TIMESTAMP WITHOUT TIME ZONE, \n'
        '    _updated TIMESTAMP WITHOUT TIME ZONE, \n'
        '    _id UUID NOT NULL, \n'
        '    _revision TEXT, \n'
        '    "someText" TEXT, \n'
        '    "someInteger" INTEGER, \n'
        '    "someNumber" FLOAT, \n'
        '    PRIMARY KEY (_id), \n'
        '    UNIQUE ("someNumber")\n'
        ');\n'
        '\n'
        'CREATE INDEX "ix_migrate/example/Multi__txn" ON "migrate/example/Multi" '
        '(_txn);\n'
        '\n'
        'ALTER TABLE "migrate/example/Multi" ADD CONSTRAINT '
        '"migrate/example/Multi_someText_someNumber_key" UNIQUE ("someText", '
        '"someNumber");\n'
        '\n'
        'CREATE TABLE "migrate/example/Multi/:changelog" (\n'
        '    _id BIGSERIAL NOT NULL, \n'
        '    _revision VARCHAR, \n'
        '    _txn UUID, \n'
        '    _rid UUID, \n'
        '    datetime TIMESTAMP WITHOUT TIME ZONE, \n'
        '    action VARCHAR(8), \n'
        '    data JSONB, \n'
        '    PRIMARY KEY (_id)\n'
        ');\n'
        '\n'
        'CREATE INDEX "ix_migrate/example/Multi/:changelog__txn" ON '
        '"migrate/example/Multi/:changelog" (_txn);\n'
        '\n'
        'COMMIT;\n'
        '\n')

    cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv'
    ])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog', 'migrate/example/Multi',
                'migrate/example/Multi/:changelog'}.issubset(tables.keys())

        table_test = tables['migrate/example/Test']
        columns_test = table_test.columns
        assert {'someText'}.issubset(columns_test.keys())
        constraint_columns = _get_table_unique_constraint_columns(table_test)
        assert any(columns == ["someText"] for columns in constraint_columns)

        table_multi = tables['migrate/example/Multi']
        columns_multi = table_multi.columns
        assert {'someText', 'someInteger', 'someNumber'}.issubset(columns_multi.keys())
        constraint_columns = _get_table_unique_constraint_columns(table_multi)
        assert any(columns == ["someNumber"] for columns in constraint_columns)
        assert any(sorted(columns) == sorted(["someNumber", "someText"]) for columns in constraint_columns)

    override_manifest(context, tmp_path, '''
     d               | r | b | m     | property     | type          | ref
     migrate/example |   |   |       |              |               |
                     |   |   | Test  |              |               |
                     |   |   |       | someText     | string        |
                     |   |   |       |              |               |
                     |   |   | Multi |              |               | someText, someNumber, someInteger
                     |   |   |       | someText     | string        |
                     |   |   |       | someInteger  | integer       |
                     |   |   |       | someNumber   | number unique |
    ''')

    result = cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv', '-p'
    ])

    assert result.output.endswith(
        'BEGIN;\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" DROP CONSTRAINT '
        '"migrate/example/Test_someText_key";\n'
        '\n'
        'ALTER TABLE "migrate/example/Multi" ADD CONSTRAINT '
        '"migrate/example/Multi_someText_someNumber_someInteger_key" UNIQUE '
        '("someText", "someNumber", "someInteger");\n'
        '\n'
        'ALTER TABLE "migrate/example/Multi" DROP CONSTRAINT '
        '"migrate/example/Multi_someText_someNumber_key";\n'
        '\n'
        'COMMIT;\n'
        '\n')
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog', 'migrate/example/Multi',
                'migrate/example/Multi/:changelog'}.issubset(tables.keys())

        table_test = tables['migrate/example/Test']
        constraint_columns = _get_table_unique_constraint_columns(table_test)
        assert any(columns == ["someText"] for columns in constraint_columns)

        table_multi = tables['migrate/example/Multi']
        constraint_columns = _get_table_unique_constraint_columns(table_multi)
        assert any(columns == ["someNumber"] for columns in constraint_columns)
        assert any(sorted(columns) == sorted(["someNumber", "someText"]) for columns in constraint_columns)

        assert not any(
            sorted(columns) == sorted(["someNumber", "someText", "someInteger"]) for columns in constraint_columns)

    cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv'
    ])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog', 'migrate/example/Multi',
                'migrate/example/Multi/:changelog'}.issubset(tables.keys())

        table_test = tables['migrate/example/Test']
        constraint_columns = _get_table_unique_constraint_columns(table_test)
        assert not any(columns == ["someText"] for columns in constraint_columns)

        table_multi = tables['migrate/example/Multi']
        constraint_columns = _get_table_unique_constraint_columns(table_multi)
        assert any(columns == ["someNumber"] for columns in constraint_columns)
        assert any(
            sorted(columns) == sorted(["someNumber", "someText", "someInteger"]) for columns in constraint_columns)

        assert not any(sorted(columns) == sorted(["someNumber", "someText"]) for columns in constraint_columns)

        _clean_up_tables(meta, ['migrate/example/Test', 'migrate/example/Test/:changelog', 'migrate/example/Multi',
                                'migrate/example/Multi/:changelog'])


def test_migrate_add_unique_constraint(
    postgresql_migration: URL,
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = '''
     d               | r | b | m    | property     | type
     migrate/example |   |   |      |              |
                     |   |   | Test |              |
                     |   |   |      | someText     | string
    '''
    context, rc = _configure(rc, tmp_path, initial_manifest)

    cli.invoke(rc, [
        'bootstrap', f'{tmp_path}/manifest.csv'
    ])

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog'}.issubset(tables.keys())
        columns = tables['migrate/example/Test'].columns
        assert {'someText'}.issubset(columns.keys())

        some_text = columns['someText']
        assert isinstance(some_text.type, sa.String)
        assert some_text.nullable

    override_manifest(context, tmp_path, '''
     d               | r | b | m    | property     | type
     migrate/example |   |   |      |              |
                     |   |   | Test |              |
                     |   |   |      | someText     | string unique
    ''')

    result = cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv', '-p'
    ])
    assert result.output.endswith(
        'BEGIN;\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" ADD CONSTRAINT '
        '"migrate/example/Test_someText_key" UNIQUE ("someText");\n'
        '\n'
        'COMMIT;\n'
        '\n')

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog'}.issubset(tables.keys())

        constraint_columns = _get_table_unique_constraint_columns(tables['migrate/example/Test'])
        assert not any(columns == ["someText"] for columns in constraint_columns)

    cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv'
    ])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog'}.issubset(tables.keys())

        constraint_columns = _get_table_unique_constraint_columns(tables['migrate/example/Test'])
        assert any(columns == ["someText"] for columns in constraint_columns)

        _clean_up_tables(meta, ['migrate/example/Test', 'migrate/example/Test/:changelog'])


def test_migrate_remove_unique_constraint(
    postgresql_migration: URL,
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = '''
     d               | r | b | m    | property     | type
     migrate/example |   |   |      |              |
                     |   |   | Test |              |
                     |   |   |      | someText     | string unique
    '''
    context, rc = _configure(rc, tmp_path, initial_manifest)

    cli.invoke(rc, [
        'bootstrap', f'{tmp_path}/manifest.csv'
    ])

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog'}.issubset(tables.keys())
        columns = tables['migrate/example/Test'].columns
        assert {'someText'}.issubset(columns.keys())

        some_text = columns['someText']
        assert isinstance(some_text.type, sa.String)
        assert some_text.nullable

        constraint_columns = _get_table_unique_constraint_columns(tables['migrate/example/Test'])
        assert any(columns == ["someText"] for columns in constraint_columns)

    override_manifest(context, tmp_path, '''
     d               | r | b | m    | property     | type
     migrate/example |   |   |      |              |
                     |   |   | Test |              |
                     |   |   |      | someText     | string
    ''')

    result = cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv', '-p'
    ])
    assert result.output.endswith(
        'BEGIN;\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" DROP CONSTRAINT '
        '"migrate/example/Test_someText_key";\n'
        '\n'
        'COMMIT;\n'
        '\n')

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog'}.issubset(tables.keys())

        constraint_columns = _get_table_unique_constraint_columns(tables['migrate/example/Test'])
        assert any(columns == ["someText"] for columns in constraint_columns)

    cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv'
    ])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog'}.issubset(tables.keys())

        constraint_columns = _get_table_unique_constraint_columns(tables['migrate/example/Test'])
        assert not any(columns == ["someText"] for columns in constraint_columns)

        _clean_up_tables(meta, ['migrate/example/Test', 'migrate/example/Test/:changelog'])


def test_migrate_create_models_with_ref(
    postgresql_migration: URL,
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = '''
     d               | r | b | m    | property     | type | ref | level
    '''
    context, rc = _configure(rc, tmp_path, initial_manifest)

    cli.invoke(rc, [
        'bootstrap', f'{tmp_path}/manifest.csv'
    ])

    override_manifest(context, tmp_path, '''
     d               | r | b | m      | property     | type          | ref                  | level
     migrate/example |   |   |        |              |               |                      |
                     |   |   | Test   |              |               | someText, someNumber |
                     |   |   |        | someText     | string        |                      |
                     |   |   |        | someInteger  | integer       |                      |
                     |   |   |        | someNumber   | number        |                      |
                     |   |   |        |              |               |                      |
                     |   |   | RefOne |              |               |                      |
                     |   |   |        | someText     | string        |                      |
                     |   |   |        | someRef      | ref           | Test                 | 4
                     |   |   |        |              |               |                      |
                     |   |   | RefTwo |              |               |                      |
                     |   |   |        | someText     | string        |                      |
                     |   |   |        | someRef      | ref           | Test                 | 3
    ''')

    result = cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv', '-p'
    ])
    # Adjust index order, since it can be random
    order = (
        'CREATE INDEX "ix_migrate/example/RefOne_someRef._id" ON '
        '"migrate/example/RefOne" ("someRef._id");\n'
        '\n'
        'CREATE INDEX "ix_migrate/example/RefOne__txn" ON "migrate/example/RefOne" '
        '(_txn);\n'
        '\n'
    )
    if order not in result.output:
        order = (
            'CREATE INDEX "ix_migrate/example/RefOne__txn" ON "migrate/example/RefOne" '
            '(_txn);\n'
            '\n'
            'CREATE INDEX "ix_migrate/example/RefOne_someRef._id" ON '
            '"migrate/example/RefOne" ("someRef._id");\n'
            '\n'
        )
    assert result.output.endswith(
        'BEGIN;\n'
        '\n'
        'CREATE TABLE "migrate/example/Test" (\n'
        '    _txn UUID, \n'
        '    _created TIMESTAMP WITHOUT TIME ZONE, \n'
        '    _updated TIMESTAMP WITHOUT TIME ZONE, \n'
        '    _id UUID NOT NULL, \n'
        '    _revision TEXT, \n'
        '    "someText" TEXT, \n'
        '    "someInteger" INTEGER, \n'
        '    "someNumber" FLOAT, \n'
        '    PRIMARY KEY (_id)\n'
        ');\n'
        '\n'
        'CREATE INDEX "ix_migrate/example/Test__txn" ON "migrate/example/Test" '
        '(_txn);\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" ADD CONSTRAINT '
        '"migrate/example/Test_someText_someNumber_key" UNIQUE ("someText", '
        '"someNumber");\n'
        '\n'
        'CREATE TABLE "migrate/example/Test/:changelog" (\n'
        '    _id BIGSERIAL NOT NULL, \n'
        '    _revision VARCHAR, \n'
        '    _txn UUID, \n'
        '    _rid UUID, \n'
        '    datetime TIMESTAMP WITHOUT TIME ZONE, \n'
        '    action VARCHAR(8), \n'
        '    data JSONB, \n'
        '    PRIMARY KEY (_id)\n'
        ');\n'
        '\n'
        'CREATE INDEX "ix_migrate/example/Test/:changelog__txn" ON '
        '"migrate/example/Test/:changelog" (_txn);\n'
        '\n'
        'CREATE TABLE "migrate/example/RefOne" (\n'
        '    _txn UUID, \n'
        '    _created TIMESTAMP WITHOUT TIME ZONE, \n'
        '    _updated TIMESTAMP WITHOUT TIME ZONE, \n'
        '    _id UUID NOT NULL, \n'
        '    _revision TEXT, \n'
        '    "someText" TEXT, \n'
        '    "someRef._id" UUID, \n'
        '    PRIMARY KEY (_id)\n'
        ');\n'
        '\n'
        f'{order}'
        'CREATE TABLE "migrate/example/RefOne/:changelog" (\n'
        '    _id BIGSERIAL NOT NULL, \n'
        '    _revision VARCHAR, \n'
        '    _txn UUID, \n'
        '    _rid UUID, \n'
        '    datetime TIMESTAMP WITHOUT TIME ZONE, \n'
        '    action VARCHAR(8), \n'
        '    data JSONB, \n'
        '    PRIMARY KEY (_id)\n'
        ');\n'
        '\n'
        'CREATE INDEX "ix_migrate/example/RefOne/:changelog__txn" ON '
        '"migrate/example/RefOne/:changelog" (_txn);\n'
        '\n'
        'CREATE TABLE "migrate/example/RefTwo" (\n'
        '    _txn UUID, \n'
        '    _created TIMESTAMP WITHOUT TIME ZONE, \n'
        '    _updated TIMESTAMP WITHOUT TIME ZONE, \n'
        '    _id UUID NOT NULL, \n'
        '    _revision TEXT, \n'
        '    "someText" TEXT, \n'
        '    "someRef.someText" TEXT, \n'
        '    "someRef.someNumber" FLOAT, \n'
        '    PRIMARY KEY (_id)\n'
        ');\n'
        '\n'
        'CREATE INDEX "ix_migrate/example/RefTwo__txn" ON "migrate/example/RefTwo" '
        '(_txn);\n'
        '\n'
        'CREATE TABLE "migrate/example/RefTwo/:changelog" (\n'
        '    _id BIGSERIAL NOT NULL, \n'
        '    _revision VARCHAR, \n'
        '    _txn UUID, \n'
        '    _rid UUID, \n'
        '    datetime TIMESTAMP WITHOUT TIME ZONE, \n'
        '    action VARCHAR(8), \n'
        '    data JSONB, \n'
        '    PRIMARY KEY (_id)\n'
        ');\n'
        '\n'
        'CREATE INDEX "ix_migrate/example/RefTwo/:changelog__txn" ON '
        '"migrate/example/RefTwo/:changelog" (_txn);\n'
        '\n'
        'ALTER TABLE "migrate/example/RefOne" ADD CONSTRAINT '
        '"fk_migrate/example/RefOne_someRef._id" FOREIGN KEY("someRef._id") '
        'REFERENCES "migrate/example/Test" (_id);\n'
        '\n'
        'COMMIT;\n'
        '\n')

    cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv'
    ])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog', 'migrate/example/RefOne',
                'migrate/example/RefOne/:changelog', 'migrate/example/RefTwo',
                'migrate/example/RefTwo/:changelog'}.issubset(tables.keys())
        columns = tables['migrate/example/Test'].columns
        assert {'someText', 'someNumber', 'someInteger'}.issubset(columns.keys())

        columns = tables['migrate/example/RefOne'].columns
        assert {'someText', 'someRef._id'}.issubset(columns.keys())
        assert not {'someRef.someText', 'someRef.someNumber'}.issubset(columns.keys())

        columns = _get_table_foreign_key_constraint_columns(tables['migrate/example/RefOne'])
        assert any(
            [['someRef._id'], ['_id']] == [constraint["column_names"], constraint["referred_column_names"]] for
            constraint in columns
        )

        columns = tables['migrate/example/RefTwo'].columns
        assert {'someText', 'someRef.someText', 'someRef.someNumber'}.issubset(columns.keys())
        assert not {'someRef._id'}.issubset(columns.keys())

        columns = _get_table_foreign_key_constraint_columns(tables['migrate/example/RefTwo'])
        assert not any(
            [['someRef._id'], ['_id']] == [constraint["column_names"],
                                           constraint["referred_column_names"]] for constraint in columns
        )

        _clean_up_tables(meta, ['migrate/example/Test', 'migrate/example/Test/:changelog', 'migrate/example/RefOne',
                                'migrate/example/RefOne/:changelog', 'migrate/example/RefTwo',
                                'migrate/example/RefTwo/:changelog'])


def test_migrate_remove_ref_column(
    postgresql_migration: URL,
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = '''
     d               | r | b | m      | property     | type          | ref                  | level
     migrate/example |   |   |        |              |               |                      |
                     |   |   | Test   |              |               | someText, someNumber |
                     |   |   |        | someText     | string        |                      |
                     |   |   |        | someInteger  | integer       |                      |
                     |   |   |        | someNumber   | number        |                      |
                     |   |   |        |              |               |                      |
                     |   |   | RefOne |              |               |                      |
                     |   |   |        | someText     | string        |                      |
                     |   |   |        | someRef      | ref           | Test                 | 4
                     |   |   |        |              |               |                      |
                     |   |   | RefTwo |              |               |                      |
                     |   |   |        | someText     | string        |                      |
                     |   |   |        | someRef      | ref           | Test                 | 3
    '''
    context, rc = _configure(rc, tmp_path, initial_manifest)

    cli.invoke(rc, [
        'bootstrap', f'{tmp_path}/manifest.csv'
    ])

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog', 'migrate/example/RefOne',
                'migrate/example/RefOne/:changelog', 'migrate/example/RefTwo',
                'migrate/example/RefTwo/:changelog'}.issubset(tables.keys())
        columns = tables['migrate/example/Test'].columns
        assert {'someText', 'someNumber', 'someInteger'}.issubset(columns.keys())

        columns = tables['migrate/example/RefOne'].columns
        assert {'someText', 'someRef._id'}.issubset(columns.keys())

        columns = _get_table_foreign_key_constraint_columns(tables['migrate/example/RefOne'])
        assert any(
            [['someRef._id'], ['_id']] == [constraint["column_names"], constraint["referred_column_names"]] for
            constraint in columns
        )

        columns = tables['migrate/example/RefTwo'].columns
        assert {'someText', 'someRef.someText', 'someRef.someNumber'}.issubset(columns.keys())

    override_manifest(context, tmp_path, '''
     d               | r | b | m      | property     | type          | ref                  | level
     migrate/example |   |   |        |              |               |                      |
                     |   |   | Test   |              |               | someText, someNumber |
                     |   |   |        | someText     | string        |                      |
                     |   |   |        | someInteger  | integer       |                      |
                     |   |   |        | someNumber   | number        |                      |
                     |   |   |        |              |               |                      |
                     |   |   | RefOne |              |               |                      |
                     |   |   |        | someText     | string        |                      |
                     |   |   |        |              |               |                      |
                     |   |   | RefTwo |              |               |                      |
                     |   |   |        | someText     | string        |                      |
                     |   |   |        | someRef      | ref           | Test                 | 3
    ''')

    result = cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv', '-p'
    ])
    #pprint(result.output)
    assert result.output.endswith(
        'BEGIN;\n'
        '\n'
        'ALTER TABLE "migrate/example/RefOne" RENAME "someRef._id" TO '
        '"__someRef._id";\n'
        '\n'
        'DROP INDEX "ix_migrate/example/RefOne_someRef._id";\n'
        '\n'
        'ALTER TABLE "migrate/example/RefOne" DROP CONSTRAINT '
        '"fk_migrate/example/RefOne_someRef._id";\n'
        '\n'
        'COMMIT;\n'
        '\n')

    cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv'
    ])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables

        columns = tables['migrate/example/RefOne'].columns
        assert {'someText', '__someRef._id'}.issubset(columns.keys())
        assert not {'someRef._id'}.issubset(columns.keys())

        columns = _get_table_foreign_key_constraint_columns(tables['migrate/example/RefOne'])
        assert not any(
            [['someRef._id'], ['_id']] == [constraint["column_names"], constraint["referred_column_names"]] for
            constraint in columns
        )
        assert not any(
            [['__someRef._id'], ['_id']] == [constraint["column_names"], constraint["referred_column_names"]] for
            constraint in columns
        )

    override_manifest(context, tmp_path, '''
         d               | r | b | m      | property     | type          | ref                  | level
         migrate/example |   |   |        |              |               |                      |
                         |   |   | Test   |              |               | someText, someNumber |
                         |   |   |        | someText     | string        |                      |
                         |   |   |        | someInteger  | integer       |                      |
                         |   |   |        | someNumber   | number        |                      |
                         |   |   |        |              |               |                      |
                         |   |   | RefOne |              |               |                      |
                         |   |   |        | someText     | string        |                      |
                         |   |   |        |              |               |                      |
                         |   |   | RefTwo |              |               |                      |
                         |   |   |        | someText     | string        |                      |
        ''')

    result = cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv', '-p'
    ])
    assert result.output.endswith(
        'BEGIN;\n'
        '\n'
        'ALTER TABLE "migrate/example/RefTwo" RENAME "someRef.someText" TO '
        '"__someRef.someText";\n'
        '\n'
        'ALTER TABLE "migrate/example/RefTwo" RENAME "someRef.someNumber" TO '
        '"__someRef.someNumber";\n'
        '\n'
        'COMMIT;\n'
        '\n')

    cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv'
    ])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables

        columns = tables['migrate/example/RefTwo'].columns
        assert {'someText', '__someRef.someText', '__someRef.someNumber'}.issubset(columns.keys())
        assert not {'someRef.someText', 'someRef.someNumber'}.issubset(columns.keys())

        _clean_up_tables(meta, ['migrate/example/Test', 'migrate/example/Test/:changelog', 'migrate/example/RefOne',
                                'migrate/example/RefOne/:changelog', 'migrate/example/RefTwo',
                                'migrate/example/RefTwo/:changelog'])


def test_migrate_adjust_ref_levels(
    postgresql_migration: URL,
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = '''
     d               | r | b | m      | property     | type          | ref                  | level
     migrate/example |   |   |        |              |               |                      |
                     |   |   | Test   |              |               | someText, someNumber |
                     |   |   |        | someText     | string        |                      |
                     |   |   |        | someInteger  | integer       |                      |
                     |   |   |        | someNumber   | number        |                      |
                     |   |   |        |              |               |                      |
                     |   |   | Ref    |              |               |                      |
                     |   |   |        | someText     | string        |                      |
                     |   |   |        | someRef      | ref           | Test                 | 4
    '''
    context, rc = _configure(rc, tmp_path, initial_manifest)

    cli.invoke(rc, [
        'bootstrap', f'{tmp_path}/manifest.csv'
    ])
    insert_values = [
        {
            "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e",
            "someText": "test1",
            "someInteger": 0,
            "someNumber": 1.0
        },
        {
            "_id": "b574111d-bd2f-4c94-9249-d9a0de49bd5b",
            "someText": "test2",
            "someInteger": 1,
            "someNumber": 2.0
        },
        {
            "_id": "1686c00c-0c59-413a-aa30-f5605488cc77",
            "someText": "test3",
            "someInteger": 0,
            "someNumber": 1.0
        },
    ]
    ref_insert = [
        {
            "_id": "350e7a87-28a5-4645-a659-2daa8e4bbe55",
            "someRef._id": "197109d9-add8-49a5-ab19-3ddc7589ce7e"
        },
        {
            "_id": "72d5dc33-a074-43f0-882f-e06abd34113b",
            "someRef._id": "b574111d-bd2f-4c94-9249-d9a0de49bd5b"
        },
        {
            "_id": "478be0be-6ab9-4c03-8551-53d881567743",
            "someRef._id": "1686c00c-0c59-413a-aa30-f5605488cc77"
        },
    ]

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        table = tables['migrate/example/Test']
        for item in insert_values:
            conn.execute(table.insert().values(item))

        result = conn.execute(table.select())
        for i, item in enumerate(result):
            item = dict(item)
            assert item["_id"] == insert_values[i]["_id"]
            assert item["someText"] == insert_values[i]["someText"]
            assert item["someInteger"] == insert_values[i]["someInteger"]
            assert item["someNumber"] == insert_values[i]["someNumber"]

        assert {'migrate/example/Test', 'migrate/example/Test/:changelog', 'migrate/example/Ref',
                'migrate/example/Ref/:changelog'}.issubset(tables.keys())
        columns = table.columns
        assert {'someText', 'someNumber', 'someInteger'}.issubset(columns.keys())

        table = tables['migrate/example/Ref']
        for item in ref_insert:
            conn.execute(table.insert().values(item))

        result = conn.execute(table.select())
        for i, item in enumerate(result):
            item = dict(item)
            assert item["_id"] == ref_insert[i]["_id"]
            assert item["someRef._id"] == insert_values[i]["_id"]
        columns = table.columns
        assert {'someText', 'someRef._id'}.issubset(columns.keys())

        columns = _get_table_foreign_key_constraint_columns(table)
        assert any(
            [['someRef._id'], ['_id']] == [constraint["column_names"], constraint["referred_column_names"]] for
            constraint in columns
        )

    override_manifest(context, tmp_path, '''
     d               | r | b | m      | property     | type          | ref                  | level
     migrate/example |   |   |        |              |               |                      |
                     |   |   | Test   |              |               | someText, someNumber |
                     |   |   |        | someText     | string        |                      |
                     |   |   |        | someInteger  | integer       |                      |
                     |   |   |        | someNumber   | number        |                      |
                     |   |   |        |              |               |                      |
                     |   |   | Ref    |              |               |                      |
                     |   |   |        | someText     | string        |                      |
                     |   |   |        | someRef      | ref           | Test                 | 3
    ''')

    result = cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv', '-p'
    ])
    assert result.output.endswith(
        'BEGIN;\n'
        '\n'
        'ALTER TABLE "migrate/example/Ref" ADD COLUMN "someRef.someText" TEXT;\n'
        '\n'
        'ALTER TABLE "migrate/example/Ref" ADD COLUMN "someRef.someNumber" FLOAT;\n'
        '\n'
        'UPDATE "migrate/example/Ref" AS old\n'
        '        SET \n'
        '"someRef.someText" = new."someText",\n'
        '"someRef.someNumber" = new."someNumber"\n'
        '        FROM "migrate/example/Test" as new\n'
        '        WHERE old."someRef._id" = new."_id";\n'
        '\n'
        'ALTER TABLE "migrate/example/Ref" RENAME "someRef._id" TO "__someRef._id";\n'
        '\n'
        'DROP INDEX "ix_migrate/example/Ref_someRef._id";\n'
        '\n'
        'ALTER TABLE "migrate/example/Ref" DROP CONSTRAINT '
        '"fk_migrate/example/Ref_someRef._id";\n'
        '\n'
        'COMMIT;\n'
        '\n')

    cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv'
    ])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        table = tables['migrate/example/Ref']
        columns = table.columns
        assert {'someText', '__someRef._id', 'someRef.someText', 'someRef.someNumber'}.issubset(columns.keys())
        assert not {'someRef._id'}.issubset(columns.keys())

        columns = _get_table_foreign_key_constraint_columns(table)
        assert not any(
            [['someRef._id'], ['_id']] == [constraint["column_names"], constraint["referred_column_names"]] for
            constraint in columns
        )
        assert not any(
            [['__someRef._id'], ['_id']] == [constraint["column_names"], constraint["referred_column_names"]] for
            constraint in columns
        )

        result = conn.execute(table.select())
        for i, item in enumerate(result):
            item = dict(item)
            assert item["_id"] == ref_insert[i]["_id"]
            assert item["someRef.someText"] == insert_values[i]["someText"]
            assert item["someRef.someNumber"] == insert_values[i]["someNumber"]

    override_manifest(context, tmp_path, '''
     d               | r | b | m      | property     | type          | ref                  | level
     migrate/example |   |   |        |              |               |                      |
                     |   |   | Test   |              |               | someText, someNumber |
                     |   |   |        | someText     | string        |                      |
                     |   |   |        | someInteger  | integer       |                      |
                     |   |   |        | someNumber   | number        |                      |
                     |   |   |        |              |               |                      |
                     |   |   | Ref    |              |               |                      |
                     |   |   |        | someText     | string        |                      |
                     |   |   |        | someRef      | ref           | Test                 | 4
    ''')

    result = cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv', '-p'
    ])
    pprint(result.output)
    assert result.output.endswith(
        'BEGIN;\n'
        '\n'
        'ALTER TABLE "migrate/example/Ref" ADD COLUMN "someRef._id" UUID;\n'
        '\n'
        'CREATE INDEX "ix_migrate/example/Ref_someRef._id" ON "migrate/example/Ref" '
        '("someRef._id");\n'
        '\n'
        'UPDATE "migrate/example/Ref" AS old\n'
        '        SET "someRef._id" = new."_id"\n'
        '        FROM "migrate/example/Test" AS new\n'
        '        WHERE \n'
        'old."someRef.someText" = new."someText" AND \n'
        'old."someRef.someNumber" = new."someNumber";\n'
        '\n'
        'ALTER TABLE "migrate/example/Ref" RENAME "someRef.someText" TO '
        '"__someRef.someText";\n'
        '\n'
        'ALTER TABLE "migrate/example/Ref" RENAME "someRef.someNumber" TO '
        '"__someRef.someNumber";\n'
        '\n'
        'ALTER TABLE "migrate/example/Ref" ADD CONSTRAINT '
        '"fk_migrate/example/Ref_someRef._id" FOREIGN KEY("someRef._id") REFERENCES '
        '"migrate/example/Test" (_id);\n'
        '\n'
        'COMMIT;\n'
        '\n')

    cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv'
    ])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables

        table = tables['migrate/example/Ref']
        columns = table.columns
        assert {'someText', '__someRef._id', 'someRef._id', '__someRef.someText', '__someRef.someNumber'}.issubset(
            columns.keys())
        assert not {'someRef.someText', 'someRef.someNumber'}.issubset(columns.keys())

        columns = _get_table_foreign_key_constraint_columns(table)
        assert any(
            [['someRef._id'], ['_id']] == [constraint["column_names"], constraint["referred_column_names"]] for
            constraint in columns
        )

        result = conn.execute(table.select())
        for i, item in enumerate(result):
            item = dict(item)
            assert item["_id"] == ref_insert[i]["_id"]
            assert item["someRef._id"] == insert_values[i]["_id"]

        _clean_up_tables(meta, ['migrate/example/Test', 'migrate/example/Test/:changelog', 'migrate/example/Ref',
                                'migrate/example/Ref/:changelog'])


def test_migrate_create_models_with_base(
    postgresql_migration: URL,
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = '''
     d               | r | b | m    | property     | type | ref | level
    '''
    context, rc = _configure(rc, tmp_path, initial_manifest)

    cli.invoke(rc, [
        'bootstrap', f'{tmp_path}/manifest.csv'
    ])

    override_manifest(context, tmp_path, '''
     d               | r | b    | m    | property     | type          | ref                  | level
     migrate/example |   |      |      |              |               |                      |
                     |   |      | Base |              |               | someText, someNumber |
                     |   |      |      | someText     | string        |                      |
                     |   |      |      | someInteger  | integer       |                      |
                     |   |      |      | someNumber   | number        |                      |
                     |   | Base |      |              |               |                      |
                     |   |      | Test |              |               |                      |
                     |   |      |      | someText     |               |                      |

    ''')

    result = cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv', '-p'
    ])
    assert result.output.endswith(
        'BEGIN;\n'
        '\n'
        'CREATE TABLE "migrate/example/Base" (\n'
        '    _txn UUID, \n'
        '    _created TIMESTAMP WITHOUT TIME ZONE, \n'
        '    _updated TIMESTAMP WITHOUT TIME ZONE, \n'
        '    _id UUID NOT NULL, \n'
        '    _revision TEXT, \n'
        '    "someText" TEXT, \n'
        '    "someInteger" INTEGER, \n'
        '    "someNumber" FLOAT, \n'
        '    PRIMARY KEY (_id)\n'
        ');\n'
        '\n'
        'CREATE INDEX "ix_migrate/example/Base__txn" ON "migrate/example/Base" '
        '(_txn);\n'
        '\n'
        'ALTER TABLE "migrate/example/Base" ADD CONSTRAINT '
        '"migrate/example/Base_someText_someNumber_key" UNIQUE ("someText", '
        '"someNumber");\n'
        '\n'
        'CREATE TABLE "migrate/example/Base/:changelog" (\n'
        '    _id BIGSERIAL NOT NULL, \n'
        '    _revision VARCHAR, \n'
        '    _txn UUID, \n'
        '    _rid UUID, \n'
        '    datetime TIMESTAMP WITHOUT TIME ZONE, \n'
        '    action VARCHAR(8), \n'
        '    data JSONB, \n'
        '    PRIMARY KEY (_id)\n'
        ');\n'
        '\n'
        'CREATE INDEX "ix_migrate/example/Base/:changelog__txn" ON '
        '"migrate/example/Base/:changelog" (_txn);\n'
        '\n'
        'CREATE TABLE "migrate/example/Test" (\n'
        '    _txn UUID, \n'
        '    _created TIMESTAMP WITHOUT TIME ZONE, \n'
        '    _updated TIMESTAMP WITHOUT TIME ZONE, \n'
        '    _id UUID NOT NULL, \n'
        '    _revision TEXT, \n'
        '    PRIMARY KEY (_id)\n'
        ');\n'
        '\n'
        'CREATE INDEX "ix_migrate/example/Test__txn" ON "migrate/example/Test" '
        '(_txn);\n'
        '\n'
        'CREATE TABLE "migrate/example/Test/:changelog" (\n'
        '    _id BIGSERIAL NOT NULL, \n'
        '    _revision VARCHAR, \n'
        '    _txn UUID, \n'
        '    _rid UUID, \n'
        '    datetime TIMESTAMP WITHOUT TIME ZONE, \n'
        '    action VARCHAR(8), \n'
        '    data JSONB, \n'
        '    PRIMARY KEY (_id)\n'
        ');\n'
        '\n'
        'CREATE INDEX "ix_migrate/example/Test/:changelog__txn" ON '
        '"migrate/example/Test/:changelog" (_txn);\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" ADD CONSTRAINT '
        '"fk_migrate/example/Base_id" FOREIGN KEY(_id) REFERENCES '
        '"migrate/example/Base" (_id);\n'
        '\n'
        'COMMIT;\n'
        '\n')

    cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv'
    ])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog', 'migrate/example/Base',
                'migrate/example/Base/:changelog'}.issubset(tables.keys())

        columns = _get_table_foreign_key_constraint_columns(tables['migrate/example/Test'])
        assert any(
            [['_id'], ['_id']] == [constraint["column_names"], constraint["referred_column_names"]] for
            constraint in columns
        )

        _clean_up_tables(meta, ['migrate/example/Test', 'migrate/example/Test/:changelog', 'migrate/example/Base',
                                'migrate/example/Base/:changelog'])


def test_migrate_remove_base_from_model(
    postgresql_migration: URL,
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = '''
 d               | r | b    | m    | property     | type          | ref                  | level
     migrate/example |   |      |      |              |               |                      |
                     |   |      | Base |              |               | someText, someNumber |
                     |   |      |      | someText     | string        |                      |
                     |   |      |      | someInteger  | integer       |                      |
                     |   |      |      | someNumber   | number        |                      |
                     |   | Base |      |              |               |                      |
                     |   |      | Test |              |               |                      |
                     |   |      |      | someText     | string        |                      |
    '''
    context, rc = _configure(rc, tmp_path, initial_manifest)

    cli.invoke(rc, [
        'bootstrap', f'{tmp_path}/manifest.csv'
    ])

    override_manifest(context, tmp_path, '''
     d               | r | b    | m    | property     | type          | ref                  | level
     migrate/example |   |      |      |              |               |                      |
                     |   |      | Base |              |               | someText, someNumber |
                     |   |      |      | someText     | string        |                      |
                     |   |      |      | someInteger  | integer       |                      |
                     |   |      |      | someNumber   | number        |                      |
                     |   |      |      |              |               |                      |
                     |   |      | Test |              |               |                      |
                     |   |      |      | someText     | string        |                      |

    ''')

    result = cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv', '-p'
    ])
    assert result.output.endswith(
        'BEGIN;\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" DROP CONSTRAINT '
        '"fk_migrate/example/Base_id";\n'
        '\n'
        'COMMIT;\n'
        '\n')

    cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv'
    ])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog', 'migrate/example/Base',
                'migrate/example/Base/:changelog'}.issubset(tables.keys())

        columns = _get_table_foreign_key_constraint_columns(tables['migrate/example/Test'])
        assert not any(
            [['_id'], ['_id']] == [constraint["column_names"], constraint["referred_column_names"]] for
            constraint in columns
        )

        _clean_up_tables(meta, ['migrate/example/Test', 'migrate/example/Test/:changelog', 'migrate/example/Base',
                                'migrate/example/Base/:changelog'])


def test_migrate_create_models_with_file_type(
    postgresql_migration: URL,
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = '''
     d               | r | b | m    | property     | type | ref | source
    '''
    context, rc = _configure(rc, tmp_path, initial_manifest)

    cli.invoke(rc, [
        'bootstrap', f'{tmp_path}/manifest.csv'
    ])

    override_manifest(context, tmp_path, '''
     d               | r | b    | m    | property       | type    | ref                  | source
     migrate/example |   |      |      |                |         |                      |
                     |   |      | Test |                |         | someText, someNumber |
                     |   |      |      | someText       | string  |                      |
                     |   |      |      | someInteger    | integer |                      |
                     |   |      |      | someNumber     | number  |                      |
                     |   |      |      | flag           | file    |                      |
                     |   |      |      | new            | file    |                      | file()
    ''')

    result = cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv', '-p'
    ])
    assert result.output.endswith(
        'BEGIN;\n'
        '\n'
        'CREATE TABLE "migrate/example/Test/:file/flag" (\n'
        '    _id UUID NOT NULL, \n'
        '    _block BYTEA, \n'
        '    PRIMARY KEY (_id)\n'
        ');\n'
        '\n'
        'CREATE TABLE "migrate/example/Test/:file/new" (\n'
        '    _id UUID NOT NULL, \n'
        '    _block BYTEA, \n'
        '    PRIMARY KEY (_id)\n'
        ');\n'
        '\n'
        'CREATE TABLE "migrate/example/Test" (\n'
        '    _txn UUID, \n'
        '    _created TIMESTAMP WITHOUT TIME ZONE, \n'
        '    _updated TIMESTAMP WITHOUT TIME ZONE, \n'
        '    _id UUID NOT NULL, \n'
        '    _revision TEXT, \n'
        '    "someText" TEXT, \n'
        '    "someInteger" INTEGER, \n'
        '    "someNumber" FLOAT, \n'
        '    "flag._id" VARCHAR, \n'
        '    "flag._content_type" VARCHAR, \n'
        '    "flag._size" BIGINT, \n'
        '    "flag._bsize" INTEGER, \n'
        '    "flag._blocks" UUID[], \n'
        '    "new._id" VARCHAR, \n'
        '    "new._content_type" VARCHAR, \n'
        '    "new._size" BIGINT, \n'
        '    "new._bsize" INTEGER, \n'
        '    "new._blocks" UUID[], \n'
        '    PRIMARY KEY (_id)\n'
        ');\n'
        '\n'
        'CREATE INDEX "ix_migrate/example/Test__txn" ON "migrate/example/Test" '
        '(_txn);\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" ADD CONSTRAINT '
        '"migrate/example/Test_someText_someNumber_key" UNIQUE ("someText", '
        '"someNumber");\n'
        '\n'
        'CREATE TABLE "migrate/example/Test/:changelog" (\n'
        '    _id BIGSERIAL NOT NULL, \n'
        '    _revision VARCHAR, \n'
        '    _txn UUID, \n'
        '    _rid UUID, \n'
        '    datetime TIMESTAMP WITHOUT TIME ZONE, \n'
        '    action VARCHAR(8), \n'
        '    data JSONB, \n'
        '    PRIMARY KEY (_id)\n'
        ');\n'
        '\n'
        'CREATE INDEX "ix_migrate/example/Test/:changelog__txn" ON '
        '"migrate/example/Test/:changelog" (_txn);\n'
        '\n'
        'COMMIT;\n'
        '\n')

    cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv'
    ])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog', 'migrate/example/Test/:file/flag',
                'migrate/example/Test/:file/new'}.issubset(tables.keys())
        table = tables['migrate/example/Test']
        columns = table.columns
        assert {'someText', 'someInteger', 'someNumber', 'flag._id', 'flag._content_type', 'flag._size', 'flag._bsize',
                'flag._blocks', 'new._id', 'new._content_type', 'new._size', 'new._bsize', 'new._blocks'}.issubset(
            columns.keys())

        _clean_up_tables(meta,
                         ['migrate/example/Test', 'migrate/example/Test/:changelog', 'migrate/example/Test/:file/flag',
                          'migrate/example/Test/:file/new'])


def test_migrate_remove_file_type(
    postgresql_migration: URL,
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = '''
     d               | r | b    | m    | property       | type    | ref                  | source
     migrate/example |   |      |      |                |         |                      |
                     |   |      | Test |                |         | someText, someNumber |
                     |   |      |      | someText       | string  |                      |
                     |   |      |      | someInteger    | integer |                      |
                     |   |      |      | someNumber     | number  |                      |
                     |   |      |      | flag           | file    |                      |
                     |   |      |      | new            | file    |                      | file()
    '''
    context, rc = _configure(rc, tmp_path, initial_manifest)

    cli.invoke(rc, [
        'bootstrap', f'{tmp_path}/manifest.csv'
    ])

    override_manifest(context, tmp_path, '''
     d               | r | b    | m    | property       | type    | ref                  | source
     migrate/example |   |      |      |                |         |                      |
                     |   |      | Test |                |         | someText, someNumber |
                     |   |      |      | someText       | string  |                      |
                     |   |      |      | someInteger    | integer |                      |
                     |   |      |      | someNumber     | number  |                      |
                     |   |      |      | flag           | file    |                      |
    ''')

    result = cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv', '-p'
    ])
    assert result.output.endswith(
        'BEGIN;\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" RENAME "new._id" TO "__new._id";\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" RENAME "new._content_type" TO '
        '"__new._content_type";\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" RENAME "new._size" TO "__new._size";\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" RENAME "new._bsize" TO "__new._bsize";\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" RENAME "new._blocks" TO "__new._blocks";\n'
        '\n'
        'ALTER TABLE "migrate/example/Test/:file/new" RENAME TO '
        '"migrate/example/Test/:file/__new";\n'
        '\n'
        'COMMIT;\n'
        '\n')

    cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv'
    ])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog', 'migrate/example/Test/:file/flag',
                'migrate/example/Test/:file/__new'}.issubset(tables.keys())
        assert not {'migrate/example/Test/:file/new'}.issubset(tables.keys())
        table = tables['migrate/example/Test']
        columns = table.columns
        assert {'someText', 'someInteger', 'someNumber', 'flag._id', 'flag._content_type', 'flag._size', 'flag._bsize',
                'flag._blocks', '__new._id', '__new._content_type', '__new._size', '__new._bsize',
                '__new._blocks'}.issubset(
            columns.keys())
        assert not {'new._id', 'new._content_type', 'new._size', 'new._bsize', 'new._blocks'}.issubset(
            columns.keys())

        _clean_up_tables(meta,
                         ['migrate/example/Test', 'migrate/example/Test/:changelog', 'migrate/example/Test/:file/flag',
                          'migrate/example/Test/:file/__new'])


def test_migrate_modify_geometry_type(
    postgresql_migration: URL,
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = '''
     d               | r | b    | m    | property       | type           | ref | source
     migrate/example |   |      |      |                |                |     |
                     |   |      | Test |                |                |     |
                     |   |      |      | someText       | string         |     |
                     |   |      |      | someGeo        | geometry       |     |
                     |   |      |      | someGeoLt      | geometry(3346) |     |
                     |   |      |      | someGeoWorld   | geometry(4326) |     |
    '''
    context, rc = _configure(rc, tmp_path, initial_manifest)

    cli.invoke(rc, [
        'bootstrap', f'{tmp_path}/manifest.csv'
    ])

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog'}.issubset(
            tables.keys())
        table = tables["migrate/example/Test"]
        conn.execute(table.insert().values({
            "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e",
            "someText": "Vilnius",
            "someGeo": "SRID=4326;POINT(15 15)",
            "someGeoLt": "SRID=3346;POINT(-471246.92725520115 1678519.8837915037)",
            "someGeoWorld": "SRID=4326;POINT(15 15)"
        }))

        result = conn.execute(table.select())
        for item in result:
            assert item["_id"] == "197109d9-add8-49a5-ab19-3ddc7589ce7e"
            assert item["someText"] == "Vilnius"
            some_geo_values = to_shape(item["someGeo"]).wkt[7:-1].split(" ")
            some_geo_lt_values = to_shape(item["someGeoLt"]).wkt[7:-1].split(" ")
            some_geo_world_values = to_shape(item["someGeoWorld"]).wkt[7:-1].split(" ")
            assert float_equals(float(some_geo_values[0]), 15, epsilon=1e-2)
            assert float_equals(float(some_geo_values[1]), 15, epsilon=1e-2)
            assert float_equals(float(some_geo_lt_values[0]), -471246.92725520115, epsilon=1e-2)
            assert float_equals(float(some_geo_lt_values[1]), 1678519.8837915037, epsilon=1e-2)
            assert float_equals(float(some_geo_world_values[0]), 15, epsilon=1e-2)
            assert float_equals(float(some_geo_world_values[1]), 15, epsilon=1e-2)

    override_manifest(context, tmp_path, '''
     d               | r | b    | m    | property       | type           | ref | source
     migrate/example |   |      |      |                |                |     |
                     |   |      | Test |                |                |     |
                     |   |      |      | someText       | string         |     |
                     |   |      |      | someGeo        | geometry(3346) |     |
                     |   |      |      | someGeoLt      | geometry       |     |
                     |   |      |      | someGeoWorld   | geometry(3346) |     |
    ''')

    result = cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv', '-p'
    ])
    pprint(result.output)
    assert result.output.endswith(
        'BEGIN;\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" ALTER COLUMN "someGeo" TYPE '
        'geometry(GEOMETRY,3346) USING ST_Transform("someGeo", 3346);\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" ALTER COLUMN "someGeoLt" TYPE '
        'geometry(GEOMETRY,4326) USING ST_Transform("someGeoLt", 4326);\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" ALTER COLUMN "someGeoWorld" TYPE '
        'geometry(GEOMETRY,3346) USING ST_Transform("someGeoWorld", 3346);\n'
        '\n'
        'COMMIT;\n'
        '\n')

    cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv'
    ])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables

        table = tables["migrate/example/Test"]

        result = conn.execute(table.select())
        for item in result:
            assert item["_id"] == "197109d9-add8-49a5-ab19-3ddc7589ce7e"
            assert item["someText"] == "Vilnius"
            some_geo_values = to_shape(item["someGeo"]).wkt[7:-1].split(" ")
            some_geo_lt_values = to_shape(item["someGeoLt"]).wkt[7:-1].split(" ")
            some_geo_world_values = to_shape(item["someGeoWorld"]).wkt[7:-1].split(" ")
            assert float_equals(float(some_geo_values[0]), -471246.92725520115, epsilon=1e-2)
            assert float_equals(float(some_geo_values[1]), 1678519.8837915037, epsilon=1e-2)
            assert float_equals(float(some_geo_lt_values[0]), 15, epsilon=1e-2)
            assert float_equals(float(some_geo_lt_values[1]), 15, epsilon=1e-2)
            assert float_equals(float(some_geo_world_values[0]), -471246.92725520115, epsilon=1e-2)
            assert float_equals(float(some_geo_world_values[1]), 1678519.8837915037, epsilon=1e-2)

        _clean_up_tables(meta, ['migrate/example/Test', 'migrate/example/Test/:changelog'])


def test_migrate_rename_model(
    postgresql_migration: URL,
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = '''
     d               | r | b    | m    | property       | type     | ref | source
     migrate/example |   |      |      |                |          |     |
                     |   |      | Ref  |                |          |     |
                     |   |      |      | someText       | string   |     |
                     |   |      |      |                |          |     |
                     |   |      | Test |                |          |     |
                     |   |      |      | someText       | string   |     |
                     |   |      |      | someFile       | file     |     |
                     |   |      |      | someRef        | ref      | Ref |
    '''
    context, rc = _configure(rc, tmp_path, initial_manifest)

    cli.invoke(rc, [
        'bootstrap', f'{tmp_path}/manifest.csv'
    ])

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Ref', 'migrate/example/Test', 'migrate/example/Test/:changelog',
                'migrate/example/Test/:file/someFile'}.issubset(
            tables.keys())
        table = tables["migrate/example/Test"]

        constraints = _get_table_foreign_key_constraint_columns(table)
        assert any(constraint["constraint_name"] == 'fk_migrate/example/Test_someRef._id' for constraint in constraints)

    override_manifest(context, tmp_path, '''
     d               | r | b    | m      | property       | type     | ref    | source
     migrate/example |   |      |        |                |          |        |
                     |   |      | NewRef |                |          |        |
                     |   |      |        | someText       | string   |        |
                     |   |      |        |                |          |        |
                     |   |      | New    |                |          |        |
                     |   |      |        | someText       | string   |        |
                     |   |      |        | someFile       | file     |        |
                     |   |      |        | someRef        | ref      | NewRef |
    ''')

    rename_file = {
        "migrate/example/Test": {
            "": "migrate/example/New"
        },
        "migrate/example/Ref": {
            "": "migrate/example/NewRef"
        },
    }
    path = tmp_path / 'rename.json'
    path.write_text(json.dumps(rename_file))

    result = cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv', '-p', '-r', path
    ])
    assert result.output.endswith(
        'BEGIN;\n'
        '\n'
        'ALTER TABLE "migrate/example/Ref" RENAME TO "migrate/example/NewRef";\n'
        '\n'
        'ALTER TABLE "migrate/example/Ref/:changelog" RENAME TO '
        '"migrate/example/NewRef/:changelog";\n'
        '\n'
        'ALTER SEQUENCE "migrate/example/Ref/:changelog__id_seq" RENAME TO '
        '"migrate/example/NewRef/:changelog__id_seq";\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" RENAME TO "migrate/example/New";\n'
        '\n'
        'ALTER TABLE "migrate/example/Test/:changelog" RENAME TO '
        '"migrate/example/New/:changelog";\n'
        '\n'
        'ALTER SEQUENCE "migrate/example/Test/:changelog__id_seq" RENAME TO '
        '"migrate/example/New/:changelog__id_seq";\n'
        '\n'
        'ALTER TABLE "migrate/example/Test/:file" RENAME TO '
        '"migrate/example/New/:file/someFile";\n'
        '\n'
        'DROP INDEX "ix_migrate/example/Test_someRef._id";\n'
        '\n'
        'CREATE INDEX "ix_migrate/example/New_someRef._id" ON "migrate/example/New" '
        '("someRef._id");\n'
        '\n'
        'ALTER TABLE "migrate/example/New" DROP CONSTRAINT '
        '"fk_migrate/example/Test_someRef._id";\n'
        '\n'
        'ALTER TABLE "migrate/example/New" ADD CONSTRAINT '
        '"fk_migrate/example/New_someRef._id" FOREIGN KEY("someRef._id") REFERENCES '
        '"migrate/example/NewRef" (_id);\n'
        '\n'
        'COMMIT;\n'
        '\n')

    cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv'
    ])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/New', 'migrate/example/New/:changelog',
                'migrate/example/New/:file/someFile', 'migrate/example/NewRef'}.issubset(tables.keys())

        assert not {'migrate/example/Test', 'migrate/example/Test/:changelog',
                    'migrate/example/Test/:file/someFile', 'migrate/example/Ref'}.issubset(tables.keys())

        table = tables["migrate/example/New"]

        constraints = _get_table_foreign_key_constraint_columns(table)
        assert any(constraint["constraint_name"] == 'fk_migrate/example/New_someRef._id' for constraint in constraints)
        assert not any(
            constraint["constraint_name"] == 'fk_migrate/example/Test_someRef._id' for constraint in constraints)

        _clean_up_tables(meta, ['migrate/example/New', 'migrate/example/New/:changelog',
                                'migrate/example/New/:file/someFile', 'migrate/example/NewRef'])


def test_migrate_rename_property(
    postgresql_migration: URL,
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = '''
     d               | r | b    | m    | property       | type     | ref      | level
     migrate/example |   |      |      |                |          |          |
                     |   |      | Ref  |                |          | someText |
                     |   |      |      | someText       | string   |          |
                     |   |      |      |                |          |          |
                     |   |      | Test |                |          |          |
                     |   |      |      | someText       | string   |          |
                     |   |      |      | someFile       | file     |          |
                     |   |      |      | someRef        | ref      | Ref      | 3
                     |   |      |      | someOther      | ref      | Ref      | 4
    '''
    context, rc = _configure(rc, tmp_path, initial_manifest)

    cli.invoke(rc, [
        'bootstrap', f'{tmp_path}/manifest.csv'
    ])

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Ref', 'migrate/example/Test', 'migrate/example/Test/:changelog',
                'migrate/example/Test/:file/someFile'}.issubset(
            tables.keys())
        table = tables["migrate/example/Test"]
        columns = table.columns
        assert {'someText', 'someRef.someText', 'someOther._id', 'someFile._id', 'someFile._content_type',
                'someFile._size',
                'someFile._bsize', 'someFile._blocks'}.issubset(columns.keys())

        constraints = _get_table_foreign_key_constraint_columns(table)
        assert any(
            constraint["constraint_name"] == 'fk_migrate/example/Test_someOther._id' for constraint in constraints)

    override_manifest(context, tmp_path, '''
     d               | r | b    | m    | property       | type     | ref      | level
     migrate/example |   |      |      |                |          |          |
                     |   |      | Ref  |                |          | newText  |
                     |   |      |      | newText        | string   |          |
                     |   |      |      |                |          |          |
                     |   |      | Test |                |          |          |
                     |   |      |      | newText        | string   |          |
                     |   |      |      | newFile        | file     |          |
                     |   |      |      | newRef         | ref      | Ref      | 3
                     |   |      |      | newOther       | ref      | Ref      | 4
    ''')

    rename_file = {
        "migrate/example/Test": {
            "someText": "newText",
            "someFile": "newFile",
            "someRef": "newRef",
            "someOther": "newOther"
        },
        "migrate/example/Ref": {
            "someText": "newText"
        },
    }
    path = tmp_path / 'rename.json'
    path.write_text(json.dumps(rename_file))

    result = cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv', '-p', '-r', path
    ])
    pprint(result.output)
    assert result.output.endswith(
        'BEGIN;\n'
        '\n'
        'ALTER TABLE "migrate/example/Ref" RENAME "someText" TO "newText";\n'
        '\n'
        'ALTER TABLE "migrate/example/Ref" DROP CONSTRAINT '
        '"migrate/example/Ref_someText_key";\n'
        '\n'
        'ALTER TABLE "migrate/example/Ref" ADD CONSTRAINT '
        '"migrate/example/Ref_newText_key" UNIQUE ("newText");\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" RENAME "someText" TO "newText";\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" RENAME "someFile._id" TO "newFile._id";\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" RENAME "someFile._content_type" TO '
        '"newFile._content_type";\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" RENAME "someFile._size" TO '
        '"newFile._size";\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" RENAME "someFile._bsize" TO '
        '"newFile._bsize";\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" RENAME "someFile._blocks" TO '
        '"newFile._blocks";\n'
        '\n'
        'ALTER TABLE "migrate/example/Test/:file/someFile" RENAME TO '
        '"migrate/example/Test/:file/newFile";\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" RENAME "someRef.someText" TO '
        '"newRef.newText";\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" RENAME "someOther._id" TO '
        '"newOther._id";\n'
        '\n'
        'DROP INDEX "ix_migrate/example/Test_someOther._id";\n'
        '\n'
        'CREATE INDEX "ix_migrate/example/Test_newOther._id" ON '
        '"migrate/example/Test" ("newOther._id");\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" DROP CONSTRAINT '
        '"fk_migrate/example/Test_someOther._id";\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" ADD CONSTRAINT '
        '"fk_migrate/example/Test_newOther._id" FOREIGN KEY("newOther._id") '
        'REFERENCES "migrate/example/Ref" (_id);\n'
        '\n'
        'COMMIT;\n'
        '\n')

    cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv', '-r', path
    ])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog',
                'migrate/example/Test/:file/newFile', 'migrate/example/Ref'}.issubset(tables.keys())

        table = tables["migrate/example/Ref"]
        columns = table.columns
        assert {'newText'}.issubset(
            columns.keys())
        assert not {'someText'}.issubset(
            columns.keys())

        table = tables["migrate/example/Test"]
        columns = table.columns
        assert {'newText', 'newOther._id', 'newRef.newText', 'newFile._id', 'newFile._content_type', 'newFile._size',
                'newFile._bsize', 'newFile._blocks'}.issubset(columns.keys())
        assert not {'someText', 'someOther._id', 'someRef.someText', 'someFile._id', 'someFile._content_type',
                    'someFile._size', 'someFile._bsize', 'someFile._blocks'}.issubset(columns.keys())

        constraints = _get_table_foreign_key_constraint_columns(table)
        assert any(
            constraint["constraint_name"] == 'fk_migrate/example/Test_newOther._id' for constraint in constraints)
        assert not any(
            constraint["constraint_name"] == 'fk_migrate/example/Test_someOther._id' for constraint in constraints)

        _clean_up_tables(meta, ['migrate/example/Test', 'migrate/example/Test/:changelog',
                                'migrate/example/Test/:file/newFile', 'migrate/example/Ref'])


def test_migrate_long_names(
    postgresql_migration: URL,
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = '''
     d               | r | b | m    | property     | type   | ref
    '''
    context, rc = _configure(rc, tmp_path, initial_manifest)

    cli.invoke(rc, [
        'bootstrap', f'{tmp_path}/manifest.csv'
    ])

    override_manifest(context, tmp_path, '''
     d               | r | b    | m    | property       | type           | ref | source
     migrate/example/very/very/long/dataset/name |   |      |      |                |                |     |
                     |   |      | ExtremelyLongModelName |                |                | veryLongPrimaryKeyName |
                     |   |      |      | veryLongPrimaryKeyName       | string         |     |
                     |   |      |      | veryLongGeometryPropertyName | geometry       |     |
                     |   |      |      | veryLongGeometryPropertyNameOther | geometry       |     |
    ''')

    result = cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv', '-p'
    ])
    pieces = [
        (
            'CREATE INDEX '
            '"ix_migrate/example/very/very/long/dat_a891da56_ropertyNameOther" ON '
            '"migrate/example/very/very/long/datase_0f562213_elyLongModelName" USING gist '
            '("veryLongGeometryPropertyNameOther");\n'
            '\n'
        ),
        (
            'CREATE INDEX '
            '"ix_migrate/example/very/very/long/dat_15d1f601_etryPropertyName" ON '
            '"migrate/example/very/very/long/datase_0f562213_elyLongModelName" USING gist '
            '("veryLongGeometryPropertyName");\n'
            '\n'
        ),
        (
            'CREATE INDEX "ix_migrate/example/very/very/long/datase_0f562213_elyLo_d813" '
            'ON "migrate/example/very/very/long/datase_0f562213_elyLongModelName" '
            '(_txn);\n'
            '\n'
        )
    ]

    combos = itertools.permutations(pieces, 3)
    ordered = pieces[0]
    for combo in combos:
        parsed = ''.join(combo)
        if parsed in result.output:
            ordered = parsed
            break

    assert result.output.endswith(
        'BEGIN;\n'
        '\n'
        'CREATE TABLE '
        '"migrate/example/very/very/long/datase_0f562213_elyLongModelName" (\n'
        '    _txn UUID, \n'
        '    _created TIMESTAMP WITHOUT TIME ZONE, \n'
        '    _updated TIMESTAMP WITHOUT TIME ZONE, \n'
        '    _id UUID NOT NULL, \n'
        '    _revision TEXT, \n'
        '    "veryLongPrimaryKeyName" TEXT, \n'
        '    "veryLongGeometryPropertyName" geometry(GEOMETRY,4326), \n'
        '    "veryLongGeometryPropertyNameOther" geometry(GEOMETRY,4326), \n'
        '    PRIMARY KEY (_id)\n'
        ');\n'
        '\n'
        f'{ordered}'
        'ALTER TABLE '
        '"migrate/example/very/very/long/datase_0f562213_elyLongModelName" ADD '
        'CONSTRAINT "migrate/example/very/very/long/datase_c824bbc4_imaryKeyName_key" '
        'UNIQUE ("veryLongPrimaryKeyName");\n'
        '\n'
        'CREATE TABLE '
        '"migrate/example/very/very/long/datase_d087b1e4_lName/:changelog" (\n'
        '    _id BIGSERIAL NOT NULL, \n'
        '    _revision VARCHAR, \n'
        '    _txn UUID, \n'
        '    _rid UUID, \n'
        '    datetime TIMESTAMP WITHOUT TIME ZONE, \n'
        '    action VARCHAR(8), \n'
        '    data JSONB, \n'
        '    PRIMARY KEY (_id)\n'
        ');\n'
        '\n'
        'CREATE INDEX "ix_migrate/example/very/very/long/datase_d087b1e4_lName_c8ee" '
        'ON "migrate/example/very/very/long/datase_d087b1e4_lName/:changelog" '
        '(_txn);\n'
        '\n'
        'COMMIT;\n'
        '\n')

    cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv'
    ])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        table = tables[get_pg_name("migrate/example/very/very/long/dataset/name/ExtremelyLongModelName")]

        _clean_up_tables(meta, ['migrate/example/very/very/long/dataset/name/ExtremelyLongModelName', 'migrate/example/very/very/long/dataset/name/ExtremelyLongModelName/:changelog'])


def test_migrate_rename_already_existing_property(
    postgresql_migration: URL,
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = '''
     d               | r | b    | m    | property       | type     | ref      | level
     migrate/example |   |      |      |                |          |          |
                     |   |      | Test |                |          |          |
                     |   |      |      | someText       | string   |          |
                     |   |      |      | otherText      | string   |          |
    '''
    context, rc = _configure(rc, tmp_path, initial_manifest)

    cli.invoke(rc, [
        'bootstrap', f'{tmp_path}/manifest.csv'
    ])

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog'}.issubset(
            tables.keys())
        table = tables["migrate/example/Test"]
        columns = table.columns
        assert {'someText', 'otherText'}.issubset(columns.keys())

    override_manifest(context, tmp_path, '''
     d               | r | b    | m    | property       | type     | ref      | level
     migrate/example |   |      |      |                |          |          |
                     |   |      | Test |                |          |          |
                     |   |      |      | otherText      | string   |          |
    ''')

    rename_file = {
        "migrate/example/Test": {
            "someText": "otherText",
        },
    }
    path = tmp_path / 'rename.json'
    path.write_text(json.dumps(rename_file))

    result = cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv', '-p', '-r', path
    ])
    pprint(result.output)
    assert result.output.endswith(
        'BEGIN;\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" RENAME "otherText" TO "__otherText";\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" RENAME "someText" TO "otherText";\n'
        '\n'
        'COMMIT;\n'
        '\n')

    cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv', '-r', path
    ])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog'}.issubset(tables.keys())

        table = tables["migrate/example/Test"]
        columns = table.columns
        assert {'otherText', '__otherText'}.issubset(
            columns.keys())
        assert not {'someText'}.issubset(
            columns.keys())

        _clean_up_tables(meta, ['migrate/example/Test', 'migrate/example/Test/:changelog'])


def test_migrate_text_full_remove(
    postgresql_migration: URL,
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = '''
     d               | r | b    | m    | property       | type     | ref      | level
     migrate/example |   |      |      |                |          |          |
                     |   |      | Test |                |          |          |
                     |   |      |      | text@lt        | string   |          |
    '''
    context, rc = _configure(rc, tmp_path, initial_manifest)

    cli.invoke(rc, [
        'bootstrap', f'{tmp_path}/manifest.csv'
    ])

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog'}.issubset(
            tables.keys())
        table = tables["migrate/example/Test"]
        columns = table.columns
        assert {'text'}.issubset(columns.keys())

    override_manifest(context, tmp_path, '''
     d               | r | b    | m    | property       | type     | ref      | level
     migrate/example |   |      |      |                |          |          |
                     |   |      | Test |                |          |          |
                     |   |      |      | new            | string   |          |
    ''')

    rename_file = {
        "migrate/example/Test": {
        },
    }
    path = tmp_path / 'rename.json'
    path.write_text(json.dumps(rename_file))

    result = cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv', '-p', '-r', path
    ])
    pprint(result.output)
    assert result.output.endswith(
        'BEGIN;\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" RENAME text TO __text;\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" ADD COLUMN "new" TEXT;\n'
        '\n'
        'COMMIT;\n'
        '\n'
    )

    cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv', '-r', path
    ])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog'}.issubset(tables.keys())

        table = tables["migrate/example/Test"]
        columns = table.columns
        assert {'new', '__text'}.issubset(
            columns.keys())
        assert not {'text'}.issubset(
            columns.keys())

        _clean_up_tables(meta, ['migrate/example/Test', 'migrate/example/Test/:changelog'])


def test_migrate_text_to_string_simple(
    postgresql_migration: URL,
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = '''
     d               | r | b    | m    | property       | type     | ref      | level
     migrate/example |   |      |      |                |          |          |
                     |   |      | Test |                |          |          |
                     |   |      |      | text@lt        | string   |          |
    '''
    context, rc = _configure(rc, tmp_path, initial_manifest)

    cli.invoke(rc, [
        'bootstrap', f'{tmp_path}/manifest.csv'
    ])

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog'}.issubset(
            tables.keys())
        table = tables["migrate/example/Test"]
        columns = table.columns
        assert {'text'}.issubset(columns.keys())

        conn.execute(table.insert().values({
            "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e",
            "text": {
                "lt": "Test"
            },
        }))

    override_manifest(context, tmp_path, '''
     d               | r | b    | m    | property       | type     | ref      | level
     migrate/example |   |      |      |                |          |          |
                     |   |      | Test |                |          |          |
                     |   |      |      | text_lt        | string   |          |
    ''')

    rename_file = {
        "migrate/example/Test": {
            "text@lt": "text_lt",
        },
    }
    path = tmp_path / 'rename.json'
    path.write_text(json.dumps(rename_file))

    result = cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv', '-p', '-r', path
    ])
    pprint(result.output)
    assert result.output.endswith(
        'BEGIN;\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" ADD COLUMN text_lt TEXT;\n'
        '\n'
        'UPDATE "migrate/example/Test" SET text_lt=("migrate/example/Test".text ->> '
        "'lt');\n"
        '\n'
        'UPDATE "migrate/example/Test" SET text=("migrate/example/Test".text - \'lt\' '
        '|| jsonb_build_object(\'__lt\', ("migrate/example/Test".text -> \'lt\'))) '
        'WHERE "migrate/example/Test".text ? \'lt\';\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" RENAME text TO __text;\n'
        '\n'
        'COMMIT;\n'
        '\n')

    cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv', '-r', path
    ])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog'}.issubset(tables.keys())

        table = tables["migrate/example/Test"]
        columns = table.columns
        assert {'text_lt', '__text'}.issubset(
            columns.keys())
        assert not {'text'}.issubset(
            columns.keys())

        result = conn.execute(table.select())
        for row in result:
            assert row['__text'] == {
                '__lt': 'Test'
            }
            assert row['text_lt'] == 'Test'
        _clean_up_tables(meta, ['migrate/example/Test', 'migrate/example/Test/:changelog'])


def test_migrate_text_to_string_direct(
    postgresql_migration: URL,
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = '''
     d               | r | b    | m    | property       | type     | ref      | level
     migrate/example |   |      |      |                |          |          |
                     |   |      | Test |                |          |          |
                     |   |      |      | text@lt        | string   |          |
    '''
    context, rc = _configure(rc, tmp_path, initial_manifest)

    cli.invoke(rc, [
        'bootstrap', f'{tmp_path}/manifest.csv'
    ])

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog'}.issubset(
            tables.keys())
        table = tables["migrate/example/Test"]
        columns = table.columns
        assert {'text'}.issubset(columns.keys())

        conn.execute(table.insert().values({
            "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e",
            "text": {
                "lt": "Testas",
                "en": "Test"
            },
        }))

    override_manifest(context, tmp_path, '''
     d               | r | b    | m    | property       | type     | ref      | level
     migrate/example |   |      |      |                |          |          |
                     |   |      | Test |                |          |          |
                     |   |      |      | text           | string   |          |
    ''')

    rename_file = {
        "migrate/example/Test": {
        },
    }
    path = tmp_path / 'rename.json'
    path.write_text(json.dumps(rename_file))

    result = cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv', '-p', '-r', path
    ])
    pprint(result.output)
    assert result.output.endswith(
        'BEGIN;\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" ADD COLUMN text_lt TEXT;\n'
        '\n'
        'UPDATE "migrate/example/Test" SET text_lt=("migrate/example/Test".text ->> '
        "'lt');\n"
        '\n'
        'UPDATE "migrate/example/Test" SET text=("migrate/example/Test".text - \'lt\' '
        '|| jsonb_build_object(\'__lt\', ("migrate/example/Test".text -> \'lt\'))) '
        'WHERE "migrate/example/Test".text ? \'lt\';\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" RENAME text TO __text;\n'
        '\n'
        'COMMIT;\n'
        '\n')

    cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv', '-r', path
    ])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog'}.issubset(tables.keys())

        table = tables["migrate/example/Test"]
        columns = table.columns
        assert {'text_lt', '__text'}.issubset(
            columns.keys())
        assert not {'text'}.issubset(
            columns.keys())

        result = conn.execute(table.select())
        for row in result:
            assert row['__text'] == {
                '__lt': 'Test'
            }
            assert row['text_lt'] == 'Test'
        _clean_up_tables(meta, ['migrate/example/Test', 'migrate/example/Test/:changelog'])



def test_migrate_text_to_string_multi(
    postgresql_migration: URL,
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = '''
     d               | r | b    | m    | property       | type     | ref      | level
     migrate/example |   |      |      |                |          |          |
                     |   |      | Test |                |          |          |
                     |   |      |      | text@lt        | string   |          |
                     |   |      |      | text@en        | string   |          |
                     |   |      |      | text@lv        | string   |          |
    '''
    context, rc = _configure(rc, tmp_path, initial_manifest)

    cli.invoke(rc, [
        'bootstrap', f'{tmp_path}/manifest.csv'
    ])

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog'}.issubset(
            tables.keys())
        table = tables["migrate/example/Test"]
        columns = table.columns
        assert {'text'}.issubset(columns.keys())

        conn.execute(table.insert().values({
            "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e",
            "text": {
                "lt": "LT",
                "en": "EN",
                "lv": "LV"
            },
        }))

    override_manifest(context, tmp_path, '''
     d               | r | b    | m    | property       | type     | ref      | level
     migrate/example |   |      |      |                |          |          |
                     |   |      | Test |                |          |          |
                     |   |      |      | text_lt        | string   |          |
                     |   |      |      | text_en        | string   |          |
                     |   |      |      | text_lv        | string   |          |
    ''')

    rename_file = {
        "migrate/example/Test": {
            "text@lt": "text_lt",
            "text@en": "text_en",
            "text@lv": "text_lv",
        },
    }
    path = tmp_path / 'rename.json'
    path.write_text(json.dumps(rename_file))

    result = cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv', '-p', '-r', path
    ])
    pprint(result.output)
    assert result.output.endswith(
        'BEGIN;\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" ADD COLUMN text_lt TEXT;\n'
        '\n'
        'UPDATE "migrate/example/Test" SET text_lt=("migrate/example/Test".text ->> '
        "'lt');\n"
        '\n'
        'UPDATE "migrate/example/Test" SET text=("migrate/example/Test".text - \'lt\' '
        '|| jsonb_build_object(\'__lt\', ("migrate/example/Test".text -> \'lt\'))) '
        'WHERE "migrate/example/Test".text ? \'lt\';\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" ADD COLUMN text_en TEXT;\n'
        '\n'
        'UPDATE "migrate/example/Test" SET text_en=("migrate/example/Test".text ->> '
        "'en');\n"
        '\n'
        'UPDATE "migrate/example/Test" SET text=("migrate/example/Test".text - \'en\' '
        '|| jsonb_build_object(\'__en\', ("migrate/example/Test".text -> \'en\'))) '
        'WHERE "migrate/example/Test".text ? \'en\';\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" ADD COLUMN text_lv TEXT;\n'
        '\n'
        'UPDATE "migrate/example/Test" SET text_lv=("migrate/example/Test".text ->> '
        "'lv');\n"
        '\n'
        'UPDATE "migrate/example/Test" SET text=("migrate/example/Test".text - \'lv\' '
        '|| jsonb_build_object(\'__lv\', ("migrate/example/Test".text -> \'lv\'))) '
        'WHERE "migrate/example/Test".text ? \'lv\';\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" RENAME text TO __text;\n'
        '\n'
        'COMMIT;\n'
        '\n')

    cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv', '-r', path
    ])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog'}.issubset(tables.keys())

        table = tables["migrate/example/Test"]
        columns = table.columns
        assert {'text_lt', 'text_en', 'text_lv', '__text'}.issubset(
            columns.keys())
        assert not {'text'}.issubset(
            columns.keys())

        result = conn.execute(table.select())
        for row in result:
            assert row['__text'] == {
                '__lt': 'LT',
                '__en': 'EN',
                '__lv': 'LV'
            }
            assert row['text_lt'] == 'LT'
            assert row['text_lv'] == 'LV'
            assert row['text_en'] == 'EN'
        _clean_up_tables(meta, ['migrate/example/Test', 'migrate/example/Test/:changelog'])


def test_migrate_text_to_string_multi_individual(
    postgresql_migration: URL,
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path
):
    cleanup_tables(postgresql_migration)
    initial_manifest = '''
     d               | r | b    | m    | property       | type     | ref      | level
     migrate/example |   |      |      |                |          |          |
                     |   |      | Test |                |          |          |
                     |   |      |      | text@lt        | string   |          |
                     |   |      |      | text@en        | string   |          |
                     |   |      |      | text@lv        | string   |          |
    '''
    context, rc = _configure(rc, tmp_path, initial_manifest)

    cli.invoke(rc, [
        'bootstrap', f'{tmp_path}/manifest.csv'
    ])

    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog'}.issubset(
            tables.keys())
        table = tables["migrate/example/Test"]
        columns = table.columns
        assert {'text'}.issubset(columns.keys())

        conn.execute(table.insert().values({
            "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7e",
            "text": {
                "lt": "LT",
                "en": "EN",
                "lv": "LV"
            },
        }))

    override_manifest(context, tmp_path, '''
     d               | r | b    | m    | property       | type     | ref      | level
     migrate/example |   |      |      |                |          |          |
                     |   |      | Test |                |          |          |
                     |   |      |      | text_lt        | string   |          |
                     |   |      |      | text@en        | string   |          |
                     |   |      |      | text@lv        | string   |          |
    ''')

    rename_file = {
        "migrate/example/Test": {
            "text@lt": "text_lt",
        },
    }
    path = tmp_path / 'rename.json'
    path.write_text(json.dumps(rename_file))

    result = cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv', '-p', '-r', path
    ])
    pprint(result.output)
    assert result.output.endswith(
        'BEGIN;\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" ADD COLUMN text_lt TEXT;\n'
        '\n'
        'UPDATE "migrate/example/Test" SET text_lt=("migrate/example/Test".text ->> '
        "'lt');\n"
        '\n'
        'UPDATE "migrate/example/Test" SET text=("migrate/example/Test".text - \'lt\' '
        '|| jsonb_build_object(\'__lt\', ("migrate/example/Test".text -> \'lt\'))) '
        'WHERE "migrate/example/Test".text ? \'lt\';\n'
        '\n'
        'COMMIT;\n'
        '\n')

    cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv', '-r', path
    ])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog'}.issubset(tables.keys())

        table = tables["migrate/example/Test"]
        columns = table.columns
        assert {'text_lt', 'text'}.issubset(
            columns.keys())
        assert not {'__text'}.issubset(
            columns.keys())

        result = conn.execute(table.select())
        for row in result:
            assert row['text'] == {
                '__lt': 'LT',
                'en': 'EN',
                'lv': 'LV'
            }
            assert row['text_lt'] == 'LT'
        _clean_up_tables(meta, ['migrate/example/Test', 'migrate/example/Test/:changelog'])
