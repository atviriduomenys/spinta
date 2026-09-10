from typing import TYPE_CHECKING, Callable, Dict, Optional

import sqlalchemy as sa
from sqlalchemy.dialects.sqlite.base import SQLiteDialect
from sqlalchemy.engine import Engine
from sqlalchemy.engine.reflection import Inspector

from spinta.cli.helpers.message import cli_message
from spinta.components import Context

if TYPE_CHECKING:
    from alembic.operations import Operations


class SqliteMigratableDb:
    dsn: str | None

    # Private sqlalchemy fields should not be accessed directly.
    _engine: Engine | None
    _metadata: sa.MetaData | None
    _conn: sa.engine.Connection | None

    migration_table_name: str = "_migrations"
    metatable_templates: dict[str, Callable[[sa.MetaData], sa.Table]]

    def __init__(self, dsn: str | None = None, migration_table_name: str = migration_table_name):
        self.configure_engine(dsn)
        self._conn = None
        self.migration_table_name = migration_table_name

        self.metatable_templates = {
            self.migration_table_name: lambda metadata: sa.Table(
                self.migration_table_name,
                metadata,
                sa.Column("migration", sa.Text, primary_key=True),
                sa.Column("applied_at", sa.DateTime, server_default=sa.func.now()),
            )
        }

    def __enter__(self):
        assert self.dsn is not None
        assert self._conn is None
        assert self._engine is not None
        self._conn = self._engine.connect()
        return self

    def __exit__(self, *exc):
        assert self._conn is not None
        self._conn.close()
        self._conn = None

    @property
    def is_entered(self) -> bool:
        return self._conn is not None

    @property
    def conn(self) -> sa.engine.Connection:
        if self._conn is None:
            raise RuntimeError("Database connection is not open.")

        return self._conn

    @property
    def engine(self) -> sa.engine.Engine:
        if self._engine is None:
            raise RuntimeError("Database engine is not configured")

        return self._engine

    @property
    def metadata(self) -> sa.MetaData:
        if self._metadata is None:
            raise RuntimeError("Database metadata is not configured.")

        return self._metadata

    def configure_engine(self, dsn: str | None):
        if dsn is None:
            self.dsn = None
            self._engine = None
            self._metadata = None
            return

        self.dsn = dsn
        self._engine = sa.create_engine(dsn)
        self._metadata = sa.MetaData(self._engine)

    def create_table(self, table: sa.Table):
        table.create(self.engine, checkfirst=True)

    def get_table(self, name: str, create_missing: bool = True, **kwargs) -> sa.Table:
        table = self.metadata.tables.get(name)
        if table is not None:
            return table

        if not create_missing:
            raise Exception("table not found")


        table_template = self.metatable_templates.get(name)
        if table_template is None:
            table_template = self._default_table_template(name, **kwargs)

        table = table_template(self.metadata)
        self.create_table(table)
        return table

    def contains_migration(self, name: str):
        migrations = self.get_table(self.migration_table_name)

        query = sa.select([sa.func.count()]).where(migrations.c.migration == name)
        count = self.conn.execute(query).scalar()
        return count != 0

    def mark_migration(self, name: str):
        if self.contains_migration(name):
            return

        migrations = self.get_table(self.migration_table_name)
        stmt = migrations.insert().values(migration=name)
        self.conn.execute(stmt)

    def create_all_metatables(self):
        for name in self.metatable_templates.keys():
            self.get_table(name)

    def _default_table_template(self, name: str, **kwargs) -> Callable[[sa.MetaData], sa.Table]:
        raise Exception("Not implemented")


def outdated_sqlite_db(
    context: Context, sqlite_db: SqliteMigratableDb, migration: str, additional_check: Callable | None = None, **kwargs
) -> bool:
    def _check_missing_migrations() -> bool:
        if not sqlite_db.contains_migration(migration):
            return True

        if additional_check and additional_check(context, **kwargs):
            return True
        return False

    if not isinstance(sqlite_db, SqliteMigratableDb):
        return False

    if sqlite_db.is_entered:
        return _check_missing_migrations()

    with sqlite_db:
        return _check_missing_migrations()


def apply_migration_to_outdated_db(
    context: Context,
    sqlite_db: SqliteMigratableDb,
    migration: str,
    apply_migration: Callable,
    database_name: str,
    **kwargs,
):
    if not outdated_sqlite_db(context, sqlite_db, migration, None, **kwargs):
        return

    cli_message(f'\tApplying "{migration}" migration to sqlite database ("{database_name}")')
    apply_migration(context, sqlite_db, migration)
    sqlite_db.mark_migration(migration)


def migrate_table(
    engine: Engine,
    metadata: sa.MetaData,
    inspector: Inspector,
    table: sa.Table,
    *,
    renames: Optional[
        Dict[
            str,  # old column name
            str,  # new column name
        ]
    ] = None,
    copy: bool = False,
) -> None:
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    if not inspector.has_table(table.name):
        table.create()
        return

    if not _need_migrating(engine, table):
        return

    renames = renames or {}

    with engine.begin() as conn:
        ctx = MigrationContext.configure(conn)
        op = Operations(ctx)

        if (isinstance(engine.dialect, SQLiteDialect) and engine.dialect.server_version_info < (3, 36)) or copy:
            _migrate_with_insert_from_select(
                engine,
                metadata,
                inspector,
                op,
                table,
                renames,
            )
        else:
            _migrate_with_alter_table(
                inspector,
                op,
                table,
                renames,
            )


def _need_migrating(
    engine: Engine,
    new_table: sa.Table,
):
    metadata = sa.MetaData(engine)
    old_table = sa.Table(new_table.name, metadata, autoload_with=engine)

    old = {c.name for c in old_table.columns}
    new = {c.name for c in new_table.columns}

    # https://docs.python.org/3/library/stdtypes.html#set
    if not (old & new):
        raise RuntimeError(f"Can't migrate, table {new_table.name!r} is completely different, from what is expected.")

    # https://docs.python.org/3/library/stdtypes.html#set
    return bool(new - old)


def _migrate_with_alter_table(
    inspector: Inspector,
    op: "Operations",
    table: sa.Table,
    renames: Optional[Dict[str, str]],
) -> None:
    # https://alembic.sqlalchemy.org/en/latest/ops.html
    cols = inspector.get_columns(table.name)
    cols = {c["name"]: c for c in cols}
    renames = {new: old for old, new in renames.items()}
    renamed = {}
    for column in table.columns:
        if column.name in renames:
            old_name = renames[column.name]
            if old_name in cols:
                op.alter_column(table.name, old_name, new_column_name=column.name)
                renamed.update({column.name: old_name})
        if column.name not in cols and column.name not in renamed:
            op.add_column(table.name, column)

    for name, col in cols.items():
        if name not in table.columns and name not in renamed.values():
            op.drop_column(table.name, name)


def _migrate_with_insert_from_select(
    engine: Engine,
    metadata: sa.MetaData,
    inspector: Inspector,
    op: "Operations",
    table: sa.Table,
    renames: Optional[Dict[str, str]],
) -> None:
    old_indexes = inspector.get_indexes(table.name)
    for index in old_indexes:
        op.drop_index(index.name, index.table_name)
    old_table_name = f"__{table.name}"

    # Recover from a possible previous failed migration, by checking if
    # rename was already done previously.
    if not inspector.has_table(old_table_name):
        op.rename_table(table.name, old_table_name)

    old_table = sa.Table(old_table_name, metadata, autoload_with=engine)

    # Recover from a possible previous failed migration, by checking if
    # new table was already created previously.
    if not inspector.has_table(table.name):
        table.create()

    select_list = []
    insert_list = []
    renames = {new: old for old, new in renames.items()}
    for column in table.columns:
        if column.name in renames and column.name not in old_table.columns:
            source = renames[column.name]
        else:
            source = column.name
        if source in old_table.columns:
            select_list.append(old_table.c[source])
            insert_list.append(column.name)

    qry = table.insert().from_select(insert_list, sa.select(*select_list))

    engine.execute(qry)

    op.drop_table(old_table_name)
