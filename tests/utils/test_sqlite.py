import pytest
import sqlalchemy as sa
from sqlalchemy.engine.reflection import Inspector

from spinta.exceptions import (
    SqliteConnectionAlreadyOpen,
    SqliteConnectionNotOpen,
    SqliteDatabaseNotConfigured,
    SqliteTableNotFound,
)
from spinta.utils.sqlite import SqliteDatabase, SqliteMigrations, migrate_table


def test_sqlite_database_connection_context():
    database = SqliteDatabase("sqlite://")

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


def test_sqlite_database_transaction_requires_open_connection():
    database = SqliteDatabase("sqlite://")

    with pytest.raises(SqliteConnectionNotOpen):
        database.begin()


def test_sqlite_database_transaction_commit_and_rollback():
    database = SqliteDatabase("sqlite://")
    table = sa.Table("rows", database.metadata, sa.Column("id", sa.Integer, primary_key=True))
    database.create_table(table)

    with database:
        with database.begin():
            database.conn.execute(table.insert().values(id=1))

        with pytest.raises(RuntimeError, match="interrupted"):
            with database.begin():
                database.conn.execute(table.insert().values(id=2))
                raise RuntimeError("interrupted")

        assert list(database.conn.execute(sa.select(table.c.id))) == [(1,)]
        assert not database.conn.in_transaction()

        transaction = database.begin()
        database.conn.execute(table.insert().values(id=3))
        transaction.commit()
        assert list(database.conn.execute(sa.select(table.c.id).order_by(table.c.id))) == [(1,), (3,)]


def test_sqlite_database_requires_configuration():
    database = SqliteDatabase()

    with pytest.raises(SqliteDatabaseNotConfigured):
        database.engine

    with pytest.raises(SqliteDatabaseNotConfigured):
        database.metadata


def test_sqlite_database_missing_table():
    database = SqliteDatabase("sqlite://")

    with pytest.raises(SqliteTableNotFound):
        database.get_table("missing", create_missing=False)


def test_sqlite_database_requires_default_table_template():
    database = SqliteDatabase("sqlite://")

    with pytest.raises(SqliteTableNotFound):
        database.get_table("model")


@pytest.mark.parametrize("migration_table_name", ["_migrations", "schema_versions"])
@pytest.mark.parametrize("cached_table", [False, True])
def test_sqlite_database_checks_migrations_without_creating_table(migration_table_name, cached_table):
    database = SqliteDatabase("sqlite://")
    book = SqliteMigrations(database, migration_table_name=migration_table_name)
    if cached_table:
        database.get_table(migration_table_name).drop(database.engine)

    with database:
        assert not book.contains_migration("initial")
        assert not sa.inspect(database.conn).has_table(migration_table_name)


def test_sqlite_database_tracks_migrations():
    database = SqliteDatabase("sqlite://")
    book = SqliteMigrations(database)

    with database:
        assert not book.contains_migration("initial")

        book.mark_migration("initial")
        book.mark_migration("initial")

        migration_table = database.get_table(book.migration_table_name)
        migrations = database.conn.execute(sa.select([migration_table.c.migration])).fetchall()

    assert migrations == [("initial",)]


def test_sqlite_database_uses_custom_migration_table_name():
    database = SqliteDatabase("sqlite://")
    book = SqliteMigrations(database, migration_table_name="schema_versions")

    with database:
        book.mark_migration("initial")

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


def _schema(inspector: Inspector) -> dict[str, list[str]]:
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
