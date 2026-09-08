import sqlalchemy as sa

from spinta.cli.helpers.push.components import PushState
from spinta.cli.helpers.upgrade.components import Script
from spinta.cli.helpers.upgrade.scripts.push_state.helpers import apply_migration_to_push_state, requires_migration
from spinta.components import Context


def requires_push_state_rename_rev_migration(context: Context, **kwargs) -> bool:
    return requires_migration(context, Script.PUSH_REV_RENAME.value, None, **kwargs)


def push_state_rename_rev_migration(context: Context, **kwargs):
    apply_migration_to_push_state(context, Script.PUSH_REV_RENAME.value, apply_migration, **kwargs)


def apply_migration(context: Context, push_state: PushState, migration: str):
    def migrate(push_state_table: sa.Table):
        from alembic.migration import MigrationContext
        from alembic.operations import Operations

        connection = push_state.conn
        ctx = MigrationContext.configure(
            connection, opts={"target_metadata": push_state.metadata, "transactional_ddl:": True}
        )
        op = Operations(ctx)
        table_name = push_state_table.name
        op.alter_column(table_name=table_name, column_name="rev", new_column_name="checksum")

    insp = sa.inspect(push_state.engine)
    table_names = insp.get_table_names()
    push_state.metadata.reflect()
    for table in table_names:
        if table.startswith("_"):
            continue

        columns = insp.get_columns(table)
        column_names = [col["name"] for col in columns]
        if "rev" in column_names and "checksum" not in column_names:
            migrate(push_state_table=push_state.get_table(table))
