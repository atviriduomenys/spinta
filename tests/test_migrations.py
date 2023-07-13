from pathlib import Path
from pprint import pprint

from spinta.core.config import RawConfig
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.tabular import create_tabular_manifest

import sqlalchemy as sa


def configure(rc, path, dsn, manifest):
    override_manifest(path, manifest)
    return rc.fork({
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
                'dsn': dsn
            },
        },
    })


def override_manifest(path, manifest):
    path = f'{path}/manifest.csv'
    create_tabular_manifest(path, striptable(manifest))


def _clean_up_tables(meta: sa.MetaData, tables: list):
    table_list = []
    for table in tables:
        table_list.append(meta.tables[table])
    meta.drop_all(tables=table_list)


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
                'column_names': column_names,
                'referred_column_names': element_names
            })
    return constraint_columns


def test_migrate_create_simple_datatype_model(
    postgresql: str,
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path
):
    initial_manifest = '''
     d | r | b | m | property   | type    | ref     | source     | prepare
    '''
    rc = configure(rc, tmp_path, postgresql, initial_manifest)

    cli.invoke(rc, [
        'bootstrap', f'{tmp_path}/manifest.csv'
    ])

    override_manifest(tmp_path, '''
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
        'COMMIT;\n'
        '\n')

    with sa.create_engine(postgresql).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert not {'migrate/example/Test', 'migrate/example/Test/:changelog'}.issubset(tables.keys())

    cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv'
    ])
    with sa.create_engine(postgresql).connect() as conn:
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
    postgresql: str,
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path
):
    initial_manifest = '''
     d               | r | b | m    | property     | type
     migrate/example |   |   |      |              |
                     |   |   | Test |              |
                     |   |   |      | someText     | string
    '''
    rc = configure(rc, tmp_path, postgresql, initial_manifest)

    cli.invoke(rc, [
        'bootstrap', f'{tmp_path}/manifest.csv'
    ])

    with sa.create_engine(postgresql).connect() as conn:
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

    override_manifest(tmp_path, '''
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

    with sa.create_engine(postgresql).connect() as conn:
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
    with sa.create_engine(postgresql).connect() as conn:
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
    postgresql: str,
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path
):
    initial_manifest = '''
     d               | r | b | m    | property     | type
     migrate/example |   |   |      |              |
                     |   |   | Test |              |
                     |   |   |      | someText     | string
                     |   |   |      | someInteger  | integer
    '''
    rc = configure(rc, tmp_path, postgresql, initial_manifest)

    cli.invoke(rc, [
        'bootstrap', f'{tmp_path}/manifest.csv'
    ])

    with sa.create_engine(postgresql).connect() as conn:
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

    override_manifest(tmp_path, '''
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

    with sa.create_engine(postgresql).connect() as conn:
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
    with sa.create_engine(postgresql).connect() as conn:
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
    postgresql: str,
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path
):
    initial_manifest = '''
     d               | r | b | m    | property     | type
     migrate/example |   |   |      |              |
                     |   |   | Test |              |
                     |   |   |      | someText     | string
                     |   |   |      | someInteger  | integer
    '''
    rc = configure(rc, tmp_path, postgresql, initial_manifest)

    cli.invoke(rc, [
        'bootstrap', f'{tmp_path}/manifest.csv'
    ])

    with sa.create_engine(postgresql).connect() as conn:
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

    override_manifest(tmp_path, '''
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

    with sa.create_engine(postgresql).connect() as conn:
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
    with sa.create_engine(postgresql).connect() as conn:
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

    override_manifest(tmp_path, '''
         d               | r | b | m    | property     | type
         migrate/example |   |   |      |              |
                         |   |   | Test |              |
                         |   |   |      | someText     | string
                         |   |   |      | someInteger  | integer
        ''')
    cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv'
    ])
    with sa.create_engine(postgresql).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog'}.issubset(tables.keys())

        columns = tables['migrate/example/Test'].columns
        assert {'someText', 'someInteger', '__someInteger'}.issubset(columns.keys())

        some_integer = columns['someInteger']
        assert isinstance(some_integer.type, sa.Integer)
        assert some_integer.nullable

    override_manifest(tmp_path, '''
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
    with sa.create_engine(postgresql).connect() as conn:
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
    with sa.create_engine(postgresql).connect() as conn:
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
    postgresql: str,
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path
):
    initial_manifest = '''
     d               | r | b | m    | property     | type   | ref
    '''
    rc = configure(rc, tmp_path, postgresql, initial_manifest)

    cli.invoke(rc, [
        'bootstrap', f'{tmp_path}/manifest.csv'
    ])

    override_manifest(tmp_path, '''
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
    with sa.create_engine(postgresql).connect() as conn:
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

    override_manifest(tmp_path, '''
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
    with sa.create_engine(postgresql).connect() as conn:
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
    with sa.create_engine(postgresql).connect() as conn:
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
    postgresql: str,
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path
):
    initial_manifest = '''
     d               | r | b | m    | property     | type
     migrate/example |   |   |      |              |
                     |   |   | Test |              |
                     |   |   |      | someText     | string
    '''
    rc = configure(rc, tmp_path, postgresql, initial_manifest)

    cli.invoke(rc, [
        'bootstrap', f'{tmp_path}/manifest.csv'
    ])

    with sa.create_engine(postgresql).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog'}.issubset(tables.keys())
        columns = tables['migrate/example/Test'].columns
        assert {'someText'}.issubset(columns.keys())

        some_text = columns['someText']
        assert isinstance(some_text.type, sa.String)
        assert some_text.nullable

    override_manifest(tmp_path, '''
     d               | r | b | m    | property     | type
     migrate/example |   |   |      |              |
                     |   |   | Test |              |
                     |   |   |      | someText     | string unique
    ''')

    result = cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv', '-p'
    ])
    pprint(result.output)
    assert result.output.endswith(
        'BEGIN;\n'
        '\n'
        'ALTER TABLE "migrate/example/Test" ADD CONSTRAINT '
        '"migrate/example/Test_someText_key" UNIQUE ("someText");\n'
        '\n'
        'COMMIT;\n'
        '\n')

    with sa.create_engine(postgresql).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog'}.issubset(tables.keys())

        constraint_columns = _get_table_unique_constraint_columns(tables['migrate/example/Test'])
        assert not any(columns == ["someText"] for columns in constraint_columns)

    cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv'
    ])
    with sa.create_engine(postgresql).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog'}.issubset(tables.keys())

        constraint_columns = _get_table_unique_constraint_columns(tables['migrate/example/Test'])
        assert any(columns == ["someText"] for columns in constraint_columns)

        _clean_up_tables(meta, ['migrate/example/Test', 'migrate/example/Test/:changelog'])


def test_migrate_remove_unique_constraint(
    postgresql: str,
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path
):
    initial_manifest = '''
     d               | r | b | m    | property     | type
     migrate/example |   |   |      |              |
                     |   |   | Test |              |
                     |   |   |      | someText     | string unique
    '''
    rc = configure(rc, tmp_path, postgresql, initial_manifest)

    cli.invoke(rc, [
        'bootstrap', f'{tmp_path}/manifest.csv'
    ])

    with sa.create_engine(postgresql).connect() as conn:
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

    override_manifest(tmp_path, '''
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

    with sa.create_engine(postgresql).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog'}.issubset(tables.keys())

        constraint_columns = _get_table_unique_constraint_columns(tables['migrate/example/Test'])
        assert any(columns == ["someText"] for columns in constraint_columns)

    cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv'
    ])
    with sa.create_engine(postgresql).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables
        assert {'migrate/example/Test', 'migrate/example/Test/:changelog'}.issubset(tables.keys())

        constraint_columns = _get_table_unique_constraint_columns(tables['migrate/example/Test'])
        assert not any(columns == ["someText"] for columns in constraint_columns)

        _clean_up_tables(meta, ['migrate/example/Test', 'migrate/example/Test/:changelog'])


def test_migrate_create_models_with_ref(
    postgresql: str,
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path
):
    initial_manifest = '''
     d               | r | b | m    | property     | type | ref | level
    '''
    rc = configure(rc, tmp_path, postgresql, initial_manifest)

    cli.invoke(rc, [
        'bootstrap', f'{tmp_path}/manifest.csv'
    ])

    override_manifest(tmp_path, '''
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
        'CREATE INDEX "ix_migrate/example/RefOne__txn" ON "migrate/example/RefOne" '
        '(_txn);\n'
        '\n'
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
    with sa.create_engine(postgresql).connect() as conn:
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


def test_migrate_remove_ref_column(
    postgresql: str,
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path
):
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
    rc = configure(rc, tmp_path, postgresql, initial_manifest)

    cli.invoke(rc, [
        'bootstrap', f'{tmp_path}/manifest.csv'
    ])

    with sa.create_engine(postgresql).connect() as conn:
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

    override_manifest(tmp_path, '''
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
    assert result.output.endswith(
        'BEGIN;\n'
        '\n'
        'ALTER TABLE "migrate/example/RefOne" RENAME "someRef._id" TO '
        '"__someRef._id";\n'
        '\n'
        'ALTER TABLE "migrate/example/RefOne" DROP CONSTRAINT '
        '"fk_migrate/example/RefOne_someRef._id";\n'
        '\n'
        'COMMIT;\n'
        '\n')

    cli.invoke(rc, [
        'migrate', f'{tmp_path}/manifest.csv'
    ])
    with sa.create_engine(postgresql).connect() as conn:
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

    override_manifest(tmp_path, '''
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
    with sa.create_engine(postgresql).connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        tables = meta.tables

        columns = tables['migrate/example/RefTwo'].columns
        assert {'someText', '__someRef.someText', '__someRef.someNumber'}.issubset(columns.keys())
        assert not {'someRef.someText', 'someRef.someNumber'}.issubset(columns.keys())
