import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import BIGINT, ARRAY

import spinta.backends.postgresql.helpers.migrate.actions as ma
from spinta import commands
from spinta.backends.constants import BackendFeatures
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers.migrate.migrate import get_root_attr, MigratePostgresMeta
from spinta.backends.postgresql.helpers.name import name_changed, get_pg_file_name, get_pg_column_name, \
    get_pg_table_name
from spinta.components import Context
from spinta.types.datatype import File
from spinta.utils.schema import NotAvailable


@commands.migrate.register(Context, PostgreSQL, MigratePostgresMeta, sa.Table, NotAvailable, File)
def migrate(context: Context, backend: PostgreSQL, meta: MigratePostgresMeta, table: sa.Table,
            old: NotAvailable, new: File, **kwargs):
    rename = meta.rename
    inspector = meta.inspector
    handler = meta.handler

    name = new.prop.name
    pg_name = get_pg_column_name(name)
    nullable = not new.required
    table_name = get_pg_table_name(rename.get_table_name(table.name))
    pkey_type = commands.get_primary_key_type(context, new.backend)
    handler.add_action(ma.AddColumnMigrationAction(
        table_name=table_name,
        column=sa.Column(get_pg_column_name(f'{name}._id'), sa.String, nullable=nullable)
    ))
    handler.add_action(ma.AddColumnMigrationAction(
        table_name=table_name,
        column=sa.Column(get_pg_column_name(f'{name}._content_type'), sa.String, nullable=nullable)
    ))
    handler.add_action(ma.AddColumnMigrationAction(
        table_name=table_name,
        column=sa.Column(get_pg_column_name(f'{name}._size'), BIGINT, nullable=nullable)
    ))
    if BackendFeatures.FILE_BLOCKS in new.backend.features:
        handler.add_action(ma.AddColumnMigrationAction(
            table_name=table_name,
            column=sa.Column(get_pg_column_name(f'{name}._bsize'), sa.Integer, nullable=nullable)
        ))
        handler.add_action(ma.AddColumnMigrationAction(
            table_name=table_name,
            column=sa.Column(get_pg_column_name(f'{name}._blocks'), ARRAY(pkey_type, ), nullable=nullable)
        ))
    old_table = get_pg_file_name(table.name, pg_name)
    new_table = get_pg_file_name(table_name, pg_name)
    if not inspector.has_table(old_table):
        handler.add_action(ma.CreateTableMigrationAction(
            table_name=new_table,
            columns=[
                sa.Column('_id', pkey_type, primary_key=True),
                sa.Column('_block', sa.LargeBinary)
            ]
        ))


@commands.migrate.register(Context, PostgreSQL, MigratePostgresMeta, sa.Table, list, File)
def migrate(context: Context, backend: PostgreSQL, meta: MigratePostgresMeta, table: sa.Table,
            old: list, new: File, **kwargs):
    rename = meta.rename
    inspector = meta.inspector
    handler = meta.handler

    column_name = rename.get_column_name(table.name, new.prop.name)
    old_name = rename.get_old_column_name(table.name, new.prop.name)
    for item in old:
        table_name = get_pg_table_name(rename.get_table_name(table.name))
        nullable = new.required if new.required == item.nullable else None
        item_name = item.name
        new_name = item_name.replace(get_root_attr(item_name), column_name) if not item_name.startswith(
            column_name) else None
        if nullable is not None or new_name is not None:
            handler.add_action(ma.AlterColumnMigrationAction(
                table_name=table_name,
                column_name=item.name,
                nullable=nullable,
                new_column_name=new_name
            ))
    table_name = get_pg_table_name(rename.get_table_name(table.name))
    old_table = get_pg_file_name(table.name, old_name)
    new_table_old_prop = get_pg_file_name(table_name, old_name)
    new_table_new_prop = get_pg_file_name(table_name, column_name)
    if name_changed(old_name, column_name) and inspector.has_table(old_table):
        handler.add_action(
            ma.RenameTableMigrationAction(
                old_table_name=new_table_old_prop,
                new_table_name=new_table_new_prop
            )
        )
