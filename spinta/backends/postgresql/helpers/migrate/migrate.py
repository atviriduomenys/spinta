import dataclasses
from typing import Any, List, Union, Dict, Tuple

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, BIGINT, ARRAY
from sqlalchemy.engine.reflection import Inspector

import spinta.backends.postgresql.helpers.migrate.actions as ma
from spinta import commands
from spinta.backends.components import BackendFeatures
from spinta.backends.constants import TableType
from spinta.backends.helpers import get_table_name
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers import get_pg_name, get_column_name
from spinta.backends.postgresql.helpers.migrate.actions import MigrationHandler
from spinta.cli.migrate import MigrateRename
from spinta.components import Context, Model, Property
from spinta.datasets.enums import Level
from spinta.types.datatype import Ref, File, Array, Object


def check_if_renamed(old_table: str, new_table: str, old_property: str, new_property: str):
    return old_table != new_table or old_property != new_property


def drop_all_indexes_and_constraints(inspector: Inspector, table: str, new_table: str, handler: MigrationHandler):
    constraints = inspector.get_unique_constraints(table)
    removed = []
    foreign_keys = inspector.get_foreign_keys(table)
    for key in foreign_keys:
        handler.add_action(ma.DropConstraintMigrationAction(
            table_name=new_table,
            constraint_name=key["name"]
        ), True)

    for constraint in constraints:
        removed.append(constraint["name"])
        handler.add_action(
            ma.DropConstraintMigrationAction(
                table_name=new_table,
                constraint_name=constraint["name"]
            )
        )
    indexes = inspector.get_indexes(table)
    for index in indexes:
        if index["name"] not in removed:
            handler.add_action(
                ma.DropIndexMigrationAction(
                    table_name=new_table,
                    index_name=index["name"]
                )
            )


def create_changelog_table(context: Context, new: Model, handler: MigrationHandler, rename: MigrateRename):
    table_name = get_pg_name(get_table_name(new, TableType.CHANGELOG))
    pkey_type = commands.get_primary_key_type(context, new.backend)
    handler.add_action(ma.CreateTableMigrationAction(
        table_name=table_name,
        columns=[
            sa.Column('_id', BIGINT, primary_key=True, autoincrement=True),
            sa.Column('_revision', sa.String),
            sa.Column('_txn', pkey_type, index=True),
            sa.Column('_rid', pkey_type),
            sa.Column('datetime', sa.DateTime),
            sa.Column('action', sa.String(8)),
            sa.Column('data', JSONB)
        ]
    ))


def handle_new_file_type(context: Context, backend: PostgreSQL, inspector: Inspector, prop: Property, pkey_type: Any,
                         handler: MigrationHandler) -> list:
    name = get_column_name(prop)
    nullable = not prop.dtype.required
    columns = []
    columns += [
        sa.Column(f'{name}._id', sa.String, nullable=nullable),
        sa.Column(f'{name}._content_type', sa.String, nullable=nullable),
        sa.Column(f'{name}._size', BIGINT, nullable=nullable)
    ]
    if BackendFeatures.FILE_BLOCKS in prop.dtype.backend.features:
        columns += [
            sa.Column(f'{name}._bsize', sa.Integer, nullable=nullable),
            sa.Column(f'{name}._blocks', ARRAY(pkey_type, ), nullable=nullable),
        ]
    new_table = get_pg_name(get_table_name(prop, TableType.FILE))
    if not inspector.has_table(new_table):
        handler.add_action(ma.CreateTableMigrationAction(
            table_name=new_table,
            columns=[
                sa.Column('_id', pkey_type, primary_key=True),
                sa.Column('_block', sa.LargeBinary)
            ]
        ))
    return columns


def handle_new_array_type(context: Context, backend: PostgreSQL, inspector: Inspector, prop: Property, pkey_type: Any,
                          handler: MigrationHandler):
    columns = []
    if isinstance(prop.dtype, Array) and prop.dtype.items:
        if prop.list is None:
            columns.append(sa.Column(prop.place, JSONB))

        if isinstance(prop.dtype.items.dtype, File):
            new_columns = handle_new_file_type(context, backend, inspector, prop.dtype.items, pkey_type, handler)
        elif isinstance(prop.dtype.items.dtype, Array):
            new_columns = handle_new_array_type(context, backend, inspector, prop.dtype.items, pkey_type, handler)
        elif isinstance(prop.dtype.items.dtype, Object):
            new_columns = handle_new_object_type(context, backend, inspector, prop.dtype.items, pkey_type, handler)
        else:
            new_columns = commands.prepare(context, backend, prop.dtype.items)
        if not isinstance(new_columns, list):
            new_columns = [new_columns]
        for column in new_columns:
            if not isinstance(column, sa.Column):
                new_columns.remove(column)

        new_table = get_pg_name(get_table_name(prop, TableType.LIST))
        if not inspector.has_table(new_table):
            main_table_name = get_pg_name(get_table_name(prop.model))
            handler.add_action(ma.CreateTableMigrationAction(
                table_name=new_table,
                columns=[
                    sa.Column('_txn', pkey_type, index=True),
                    sa.Column('_rid', pkey_type, sa.ForeignKey(
                        f'{main_table_name}._id', ondelete='CASCADE',
                    ), index=True),
                    *new_columns
                ]
            ))
    return columns


def handle_new_object_type(context: Context, backend: PostgreSQL, inspector: Inspector, prop: Property, pkey_type: Any,
                           handler: MigrationHandler):
    columns = []
    if isinstance(prop.dtype, Object) and prop.dtype.properties:
        for new_prop in prop.dtype.properties.values():
            if prop.name.startswith('_') and prop.name not in ('_revision',):
                continue
            if isinstance(new_prop.dtype, File):
                columns = handle_new_file_type(context, backend, inspector, new_prop, pkey_type, handler)
            elif isinstance(new_prop.dtype, Array):
                columns = handle_new_array_type(context, backend, inspector, new_prop, pkey_type, handler)
            elif isinstance(new_prop.dtype, Object):
                columns = handle_new_object_type(context, backend, inspector, new_prop, pkey_type, handler)
            else:
                columns = commands.prepare(context, backend, new_prop)

            if not isinstance(columns, list):
                columns = [columns]
            for column in columns:
                if not isinstance(column, sa.Column):
                    columns.remove(column)
    return columns


def rename_index_name(index: str, old_table: str, new_table: str, old_property: str, new_property: str):
    new = f'ix_{new_table}_{new_property}'
    return new


def get_remove_name(name: str) -> str:
    new_name = name.split("/")
    if not new_name[-1].startswith("__"):
        new_name[-1] = f'__{new_name[-1]}'
    new_name = '/'.join(new_name)
    new_name = get_pg_name(new_name)
    return new_name


def get_prop_names(prop: Property):
    name = prop.name
    if isinstance(prop.dtype, Ref):
        if not prop.level or prop.level > Level.open:
            name = f'{name}._id'
        else:
            for refprop in prop.dtype.refprops:
                yield f'{name}.{refprop.name}'
    yield name


def get_root_attr(text: str):
    return text.split(".")[0].split("@")[0]


def get_last_attr(text: str):
    return text.split(".")[-1].split("@")[-1]


def json_has_key(backend: PostgreSQL, column: sa.Column, table: sa.Table, key: str):
    with backend.engine.begin() as connection:
        query = sa.select(table.select().where(column.has_key(key)).exists())
        return connection.execute(query).scalar()


def jsonb_keys(backend: PostgreSQL, column: sa.Column, table: sa.Table):
    with backend.engine.begin() as connection:
        keys = sa.func.jsonb_object_keys(
            column
        )
        query = sa.select(
            [keys]
        ).select_from(table).group_by(keys)
        return [result[0] for result in connection.execute(query)]


def name_key(name: str):
    return name


def model_name_key(model: str) -> str:
    return get_pg_name(model)


def property_and_column_name_key(item: Union[sa.Column, Property], rename, table) -> str:
    name = ''
    if isinstance(item, sa.Column):
        name = item.name
        name = rename.get_old_column_name(table, name)
    elif isinstance(item, Property):
        name = get_column_name(item)
        name = rename.get_old_column_name(table, name)
    return get_root_attr(get_pg_name(name))


@dataclasses.dataclass
class MigratePostgresMeta:
    inspector: Inspector
    rename: MigrateRename
    handler: MigrationHandler


@dataclasses.dataclass
class JSONColumnMigrateMeta:
    column: sa.Column
    keys: List[str] = dataclasses.field(default_factory=list)
    new_keys: Dict[str, str] = dataclasses.field(default_factory=dict)
    full_remove: bool = dataclasses.field(default=True)
    cast_to: Tuple[sa.Column, str] = dataclasses.field(default=None)

    def initialize(self, backend, table):
        self.keys = jsonb_keys(backend, self.column, table)


@dataclasses.dataclass
class MigrateModelMeta:
    json_columns: Dict[str, JSONColumnMigrateMeta] = dataclasses.field(default_factory=dict)

    def add_json_column(self, backend: PostgreSQL, table: sa.Table, column: sa.Column):
        meta = JSONColumnMigrateMeta(
            column=column,
        )
        meta.initialize(backend, table)
        self.json_columns[column.name] = meta

