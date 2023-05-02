from typing import Dict
from typing import Optional

import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.dialects.sqlite.base import SQLiteDialect
from sqlalchemy.engine import Engine
from sqlalchemy.engine.reflection import Inspector


def migrate_table(
    engine: Engine,
    metadata: sa.MetaData,
    inspector: Inspector,
    table: sa.Table,
    *,
    renames: Optional[Dict[
        str,  # old column name
        str,  # new column name
    ]] = None,
    copy: bool = False
) -> None:
    if not inspector.has_table(table.name):
        table.create()
        return

    renames = renames or {}

    with engine.begin() as conn:
        ctx = MigrationContext.configure(conn)
        op = Operations(ctx)

        if (
            isinstance(engine.dialect, SQLiteDialect) and
            engine.dialect.server_version_info < (3, 36)
        ) or copy:
            _migrate_with_insert_from_select(engine, metadata, op, table, renames)
        else:
            _migrate_with_alter_table(
                inspector,
                op,
                table,
                renames,
            )


def _migrate_with_alter_table(
    inspector: Inspector,
    op: Operations,
    table: sa.Table,
    renames: Optional[Dict[str, str]],
) -> None:
    # https://alembic.sqlalchemy.org/en/latest/ops.html
    cols = inspector.get_columns(table.name)
    cols = {c['name']: c for c in cols}
    renames = {new: old for old, new in renames.items()}
    renamed = {}
    for column in table.columns:
        if column.name in renames:
            old_name = renames[column.name]
            if old_name in cols:
                op.alter_column(table.name, old_name, new_column_name=column.name)
                renamed.update({
                    column.name: old_name
                })
        if (
            column.name not in cols and
            column.name not in renamed
        ):
            op.add_column(table.name, column)

    for name, col in cols.items():
        if (
            name not in table.columns and
            name not in renamed.values()
        ):
            op.drop_column(table.name, name)


def _migrate_with_insert_from_select(
    engine: Engine,
    metadata: sa.MetaData,
    op: Operations,
    table: sa.Table,
    renames: Optional[Dict[str, str]],
) -> None:
    old_table_name = f'__{table.name}'
    op.rename_table(table.name, old_table_name)
    table.create()
    old_table = sa.Table(old_table_name, metadata, autoload_with=engine)

    select_list = []
    insert_list = []
    renames = {new: old for old, new in renames.items()}
    for column in table.columns:
        if (
            column.name in renames and
            column.name not in old_table.columns
        ):
            source = renames[column.name]
        else:
            source = column.name
        if source in old_table.columns:
            select_list.append(old_table.c[source])
            insert_list.append(column.name)

    engine.execute(
        table.insert().from_select(
            insert_list,
            sa.select(*select_list)
        )
    )

    op.drop_table(old_table_name)
