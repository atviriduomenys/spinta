import itertools
import json
from pathlib import Path

import sqlalchemy as sa
from sqlalchemy.engine.url import make_url, URL

from spinta.backends.postgresql.helpers import get_pg_name
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.context import create_test_context
from spinta.testing.pytest import MIGRATION_DATABASE
from spinta.testing.tabular import create_tabular_manifest


def configure_migrate(rc, path, manifest):
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


def cleanup_table_list(meta: sa.MetaData, tables: list):
    table_list = []
    for table in tables:
        table_list.append(meta.tables[get_pg_name(table)])
    meta.drop_all(tables=table_list)


def float_equals(a: float, b: float, epsilon=1e-9):
    return abs(a - b) < epsilon


def get_table_unique_constraint_columns(table: sa.Table):
    constraint_columns = []
    for constraint in table.constraints:
        if isinstance(constraint, sa.UniqueConstraint):
            column_names = [column.name for column in constraint.columns]
            constraint_columns.append(column_names)
    return constraint_columns


def get_table_foreign_key_constraint_columns(table: sa.Table):
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
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)
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
        assert isinstance(some_text.name, sa.String)
        assert some_text.nullable

        some_integer = columns['someInteger']
        assert isinstance(some_integer.name, sa.Integer)
        assert some_integer.nullable

        some_number = columns['someNumber']
        assert isinstance(some_number.name, sa.Float)
        assert some_number.nullable

        some_date = columns['someDate']
        assert isinstance(some_date.name, sa.Date)
        assert some_date.nullable

        some_date_time = columns['someDateTime']
        assert isinstance(some_date_time.name, sa.DateTime)
        assert some_date_time.nullable

        some_time = columns['someTime']
        assert isinstance(some_time.name, sa.Time)
        assert some_time.nullable

        some_boolean = columns['someBoolean']
        assert isinstance(some_boolean.name, sa.Boolean)
        assert some_boolean.nullable

        some_url = columns['someUrl']
        assert isinstance(some_url.name, sa.String)
        assert some_url.nullable

        some_uri = columns['someUri']
        assert isinstance(some_uri.name, sa.String)
        assert some_uri.nullable

        some_binary = columns['someBinary']
        assert isinstance(some_binary.name, sa.LargeBinary)
        assert some_binary.nullable

        some_json = columns['someJson']
        assert isinstance(some_json.name, sa.JSON)
        assert some_json.nullable

        cleanup_table_list(meta, ['migrate/example/Test', 'migrate/example/Test/:changelog'])


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
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

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
        assert isinstance(some_text.name, sa.String)
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
        assert isinstance(some_integer.name, sa.Integer)
        assert some_integer.nullable

        cleanup_table_list(meta, ['migrate/example/Test', 'migrate/example/Test/:changelog'])


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
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

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
        assert isinstance(some_text.name, sa.String)
        assert some_text.nullable

        some_integer = columns['someInteger']
        assert isinstance(some_integer.name, sa.Integer)
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
        assert isinstance(some_integer.name, sa.Integer)
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
        assert isinstance(some_integer.name, sa.Integer)
        assert some_integer.nullable

        assert not {'someInteger'}.issubset(columns.keys())
        cleanup_table_list(meta, ['migrate/example/Test', 'migrate/example/Test/:changelog'])


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
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

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
        assert isinstance(some_text.name, sa.String)
        assert some_text.nullable

        some_integer = columns['someInteger']
        assert isinstance(some_integer.name, sa.Integer)
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
        assert isinstance(some_integer.name, sa.Integer)
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
        assert isinstance(some_integer.name, sa.Integer)
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
        assert isinstance(some_integer.name, sa.Integer)
        assert some_integer.nullable

        assert not {'someInteger'}.issubset(columns.keys())

        cleanup_table_list(meta, ['migrate/example/Test', 'migrate/example/Test/:changelog'])


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
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

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
        assert isinstance(some_text.name, sa.String)
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

        constraint_columns = get_table_unique_constraint_columns(tables['migrate/example/Test'])
        assert not any(columns == ["someText"] for columns in constraint_columns)

    cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv'
    ])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog'}.issubset(tables.keys())

        constraint_columns = get_table_unique_constraint_columns(tables['migrate/example/Test'])
        assert any(columns == ["someText"] for columns in constraint_columns)

        cleanup_table_list(meta, ['migrate/example/Test', 'migrate/example/Test/:changelog'])


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
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

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
        assert isinstance(some_text.name, sa.String)
        assert some_text.nullable

        constraint_columns = get_table_unique_constraint_columns(tables['migrate/example/Test'])
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

        constraint_columns = get_table_unique_constraint_columns(tables['migrate/example/Test'])
        assert any(columns == ["someText"] for columns in constraint_columns)

    cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv'
    ])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog'}.issubset(tables.keys())

        constraint_columns = get_table_unique_constraint_columns(tables['migrate/example/Test'])
        assert not any(columns == ["someText"] for columns in constraint_columns)

        cleanup_table_list(meta, ['migrate/example/Test', 'migrate/example/Test/:changelog'])


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
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

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

        columns = get_table_foreign_key_constraint_columns(tables['migrate/example/Test'])
        assert any(
            [['_id'], ['_id']] == [constraint["column_names"], constraint["referred_column_names"]] for
            constraint in columns
        )

        cleanup_table_list(meta, ['migrate/example/Test', 'migrate/example/Test/:changelog', 'migrate/example/Base',
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
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

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

        columns = get_table_foreign_key_constraint_columns(tables['migrate/example/Test'])
        assert not any(
            [['_id'], ['_id']] == [constraint["column_names"], constraint["referred_column_names"]] for
            constraint in columns
        )

        cleanup_table_list(meta, ['migrate/example/Test', 'migrate/example/Test/:changelog', 'migrate/example/Base',
                                'migrate/example/Base/:changelog'])


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
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

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

        constraints = get_table_foreign_key_constraint_columns(table)
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

        constraints = get_table_foreign_key_constraint_columns(table)
        assert any(constraint["constraint_name"] == 'fk_migrate/example/New_someRef._id' for constraint in constraints)
        assert not any(
            constraint["constraint_name"] == 'fk_migrate/example/Test_someRef._id' for constraint in constraints)

        cleanup_table_list(meta, ['migrate/example/New', 'migrate/example/New/:changelog',
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
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

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

        constraints = get_table_foreign_key_constraint_columns(table)
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

        constraints = get_table_foreign_key_constraint_columns(table)
        assert any(
            constraint["constraint_name"] == 'fk_migrate/example/Test_newOther._id' for constraint in constraints)
        assert not any(
            constraint["constraint_name"] == 'fk_migrate/example/Test_someOther._id' for constraint in constraints)

        cleanup_table_list(meta, ['migrate/example/Test', 'migrate/example/Test/:changelog',
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
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

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

        cleanup_table_list(meta, ['migrate/example/very/very/long/dataset/name/ExtremelyLongModelName',
                                'migrate/example/very/very/long/dataset/name/ExtremelyLongModelName/:changelog'])


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
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

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

        cleanup_table_list(meta, ['migrate/example/Test', 'migrate/example/Test/:changelog'])


def test_migrate_change_basic_type(
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
                     |   |      |      | someInt        | string   |          |
                     |   |      |      | someFloat      | string   |          |
    '''
    context, rc = configure_migrate(rc, tmp_path, initial_manifest)

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
        assert {'someInt', 'someFloat'}.issubset(columns.keys())
        conn.execute(table.insert().values({
            "_id": "197109d9-add8-49a5-ab19-3ddc7589ce7a",
            "someInt": "1",
            "someFloat": "1.5"
        }))

    override_manifest(context, tmp_path, '''
     d               | r | b    | m    | property       | type     | ref      | level
     migrate/example |   |      |      |                |          |          |
                     |   |      | Test |                |          |          |
                     |   |      |      | someInt        | integer  |          |
                     |   |      |      | someFloat      | number   |          |
    ''')

    result = cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv', '-p'
    ])

    assert result.output.endswith(
        'BEGIN;\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" ALTER COLUMN "someInt" TYPE INTEGER USING '
        'CAST("migrate/example/Test"."someInt" AS INTEGER);\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" ALTER COLUMN "someFloat" TYPE FLOAT USING '
        'CAST("migrate/example/Test"."someFloat" AS FLOAT);\n'
        '\n'
        'COMMIT;\n'
        '\n')

    cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv',
    ])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog'}.issubset(tables.keys())

        table = tables["migrate/example/Test"]
        columns = table.columns
        assert {'someFloat', 'someInt'}.issubset(
            columns.keys())

        result = conn.execute(table.select())
        for row in result:
            assert row['someInt'] == 1
            assert row['someFloat'] == 1.5

    override_manifest(context, tmp_path, '''
     d               | r | b    | m    | property       | type     | ref      | level
     migrate/example |   |      |      |                |          |          |
                     |   |      | Test |                |          |          |
                     |   |      |      | someInt        | string   |          |
                     |   |      |      | someFloat      | string   |          |
    ''')

    result = cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv', '-p'
    ])
    assert result.output.endswith(
        'BEGIN;\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" ALTER COLUMN "someInt" TYPE TEXT USING '
        'CAST("migrate/example/Test"."someInt" AS TEXT);\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" ALTER COLUMN "someFloat" TYPE TEXT USING '
        'CAST("migrate/example/Test"."someFloat" AS TEXT);\n'
        '\n'
        'COMMIT;\n'
        '\n')

    cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv',
    ])
    with sa.create_engine(postgresql_migration).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog'}.issubset(tables.keys())

        table = tables["migrate/example/Test"]
        columns = table.columns
        assert {'someFloat', 'someInt'}.issubset(
            columns.keys())

        result = conn.execute(table.select())
        for row in result:
            assert row['someInt'] == "1"
            assert row['someFloat'] == "1.5"
        cleanup_table_list(meta, ['migrate/example/Test', 'migrate/example/Test/:changelog'])
