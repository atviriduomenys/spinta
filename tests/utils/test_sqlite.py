from typing import Dict, List

import pytest
import sqlalchemy as sa
from sqlalchemy.engine.reflection import Inspector

from spinta.exceptions import (
    SqliteConnectionAlreadyOpen,
    SqliteConnectionNotOpen,
    SqliteDatabaseNotConfigured,
    SqliteTableNotFound,
)
from spinta.utils.sqlite import SqliteMigratableDb, migrate_table


def test_sqlite_migratable_db_connection_context():
    database = SqliteMigratableDb("sqlite://")

    assert not database.is_entered
    with pytest.raises(SqliteConnectionNotOpen):
        database.conn

    with database:
        assert database.is_entered
        assert not database.conn.closed

        with pytest.raises(SqliteConnectionAlreadyOpen):
            with database:
                pass

    assert not database.is_entered
    with pytest.raises(SqliteConnectionNotOpen):
        database.conn


def test_sqlite_migratable_db_requires_configuration():
    database = SqliteMigratableDb()

    with pytest.raises(SqliteDatabaseNotConfigured):
        database.engine

    with pytest.raises(SqliteDatabaseNotConfigured):
        database.metadata


def test_sqlite_migratable_db_missing_table():
    database = SqliteMigratableDb("sqlite://")

    with pytest.raises(SqliteTableNotFound):
        database.get_table("missing", create_missing=False)


def test_sqlite_migratable_db_requires_default_table_template():
    database = SqliteMigratableDb("sqlite://")

    with pytest.raises(NotImplementedError):
        database.get_table("model")


def test_sqlite_migratable_db_tracks_migrations():
    database = SqliteMigratableDb("sqlite://")

    with database:
        assert not database.contains_migration("initial")

        database.mark_migration("initial")
        database.mark_migration("initial")

        migration_table = database.get_table(database.migration_table_name)
        migrations = database.conn.execute(sa.select([migration_table.c.migration])).fetchall()

    assert migrations == [("initial",)]


def test_sqlite_migratable_db_uses_custom_migration_table_name():
    database = SqliteMigratableDb("sqlite://", migration_table_name="schema_versions")

    with database:
        database.mark_migration("initial")

    assert sa.inspect(database.engine).get_table_names() == ["schema_versions"]


@pytest.mark.parametrize("copy", [True, False])
def test_create_table(copy):
    engine = sa.create_engine("sqlite://")
    metadata = sa.MetaData(engine)
    columns = [
        sa.Column("column", sa.Text),
    ]
    table = sa.Table("table", metadata, *columns)
    inspector = sa.inspect(engine)
    migrate_table(engine, metadata, inspector, table, copy=copy)
    inspector = sa.inspect(engine)
    assert _schema(inspector) == {
        "table": [
            "column text null",
        ],
    }


@pytest.mark.parametrize("copy", [True, False])
def test_add_column(copy):
    engine = sa.create_engine("sqlite://")
    metadata = sa.MetaData(engine)
    columns = [
        sa.Column("column1", sa.Text),
    ]
    table = sa.Table("table", metadata, *columns)
    table.create()
    inspector = sa.inspect(engine)

    metadata = sa.MetaData(engine)
    columns = [
        sa.Column("column1", sa.Text),
        sa.Column("column2", sa.Text),
    ]
    table = sa.Table("table", metadata, *columns)

    migrate_table(engine, metadata, inspector, table, copy=copy)
    inspector = sa.inspect(engine)
    assert _schema(inspector) == {
        "table": [
            "column1 text null",
            "column2 text null",
        ],
    }


@pytest.mark.parametrize("copy", [True, False])
def test_drop_column(copy):
    engine = sa.create_engine("sqlite://")
    metadata = sa.MetaData(engine)
    columns = [
        sa.Column("column1", sa.Text),
        sa.Column("column3", sa.Text),
    ]
    table = sa.Table("table", metadata, *columns)
    table.create()
    inspector = sa.inspect(engine)

    metadata = sa.MetaData(engine)
    columns = [
        sa.Column("column1", sa.Text),
        sa.Column("column2", sa.Text),
    ]
    table = sa.Table("table", metadata, *columns)

    migrate_table(engine, metadata, inspector, table, copy=copy)
    inspector = sa.inspect(engine)
    assert _schema(inspector) == {
        "table": [
            "column1 text null",
            "column2 text null",
        ],
    }


@pytest.mark.parametrize("copy", [True, False])
def test_rename_column(copy):
    engine = sa.create_engine("sqlite://")
    metadata = sa.MetaData(engine)
    columns = [
        sa.Column("column1", sa.Text),
        sa.Column("column3", sa.Text),
    ]
    table = sa.Table("table", metadata, *columns)
    table.create()
    inspector = sa.inspect(engine)

    metadata = sa.MetaData(engine)
    columns = [
        sa.Column("column1", sa.Text),
        sa.Column("column2", sa.Text),
    ]
    table = sa.Table("table", metadata, *columns)

    migrate_table(engine, metadata, inspector, table, renames={"column3": "column2"}, copy=copy)
    inspector = sa.inspect(engine)
    assert _schema(inspector) == {
        "table": [
            "column1 text null",
            "column2 text null",
        ],
    }


@pytest.mark.parametrize("copy", [True, False])
def test_rename_missing_column(copy):
    engine = sa.create_engine("sqlite://")
    metadata = sa.MetaData(engine)
    columns = [
        sa.Column("column1", sa.Text),
        sa.Column("column2", sa.Text),
    ]
    table = sa.Table("table", metadata, *columns)
    table.create()
    inspector = sa.inspect(engine)

    metadata = sa.MetaData(engine)
    columns = [
        sa.Column("column1", sa.Text),
        sa.Column("column2", sa.Text),
    ]
    table = sa.Table("table", metadata, *columns)

    migrate_table(engine, metadata, inspector, table, renames={"column3": "column2"}, copy=copy)
    inspector = sa.inspect(engine)
    assert _schema(inspector) == {
        "table": [
            "column1 text null",
            "column2 text null",
        ],
    }


@pytest.mark.parametrize("copy", [True, False])
def test_same_table(copy):
    engine = sa.create_engine("sqlite://")
    metadata = sa.MetaData(engine)
    columns = [
        sa.Column("column1", sa.Text),
        sa.Column("column2", sa.Text),
    ]
    table = sa.Table("table", metadata, *columns)
    table.create()
    inspector = sa.inspect(engine)

    metadata = sa.MetaData(engine)
    columns = [
        sa.Column("column1", sa.Text),
        sa.Column("column2", sa.Text),
    ]
    table = sa.Table("table", metadata, *columns)

    migrate_table(engine, metadata, inspector, table, renames={"foo": "bar"}, copy=copy)
    inspector = sa.inspect(engine)
    assert _schema(inspector) == {
        "table": [
            "column1 text null",
            "column2 text null",
        ],
    }


@pytest.mark.parametrize("copy", [True, False])
def test_different_tables(copy):
    engine = sa.create_engine("sqlite://")
    metadata = sa.MetaData(engine)
    columns = [
        sa.Column("column1", sa.Text),
        sa.Column("column2", sa.Text),
    ]
    table = sa.Table("table", metadata, *columns)
    table.create()
    inspector = sa.inspect(engine)

    metadata = sa.MetaData(engine)
    columns = [
        sa.Column("column3", sa.Text),
        sa.Column("column4", sa.Text),
    ]
    table = sa.Table("table", metadata, *columns)

    with pytest.raises(RuntimeError) as e:
        migrate_table(engine, metadata, inspector, table, renames={"foo": "bar"}, copy=copy)
    assert str(e.value) == ("Can't migrate, table 'table' is completely different, from what is expected.")


def _schema(inspector: Inspector) -> Dict[str, List[str]]:
    schema = {}
    for table in inspector.get_table_names():
        columns = []
        for column in inspector.get_columns(table):
            name = column["name"]
            type = str(column["type"]).lower()
            null = " null" if column["nullable"] else ""
            pkey = " primary key" if column["primary_key"] else ""
            columns.append(f"{name} {type}{null}{pkey}")
        schema[table] = sorted(columns)
    return schema
