from typing import List, Union

import sqlalchemy as sa

from sqlalchemy.dialects.postgresql import JSONB, UUID, BIGINT, ARRAY
from sqlalchemy.sql.type_api import TypeEngine

from spinta import commands
from spinta.components import Context, Manifest, Model, Property
from spinta.types.datatype import Array, DataType, File, Object, PrimaryKey, Ref
from spinta.backends.postgresql.components import PostgreSQL, BackendFeatures
from spinta.backends.postgresql.constants import TableType, UNSUPPORTED_TYPES
from spinta.backends.postgresql.helpers import get_pg_name, get_table_name, get_changes_table
from spinta.backends.postgresql.helpers import get_column_name


@commands.prepare.register()
def prepare(context: Context, backend: PostgreSQL, manifest: Manifest):
    # Prepare backend for models.
    for model in manifest.models.values():
        if model.backend.name == backend.name:
            commands.prepare(context, backend, model)


@commands.prepare.register()
def prepare(context: Context, backend: PostgreSQL, model: Model):
    columns = []
    for prop in model.properties.values():
        # FIXME: _revision should has its own type and on database column type
        #        should bet received from get_primary_key_type() command.
        if prop.name.startswith('_') and prop.name not in ('_id', '_revision'):
            continue
        column = commands.prepare(context, backend, prop)
        if isinstance(column, list):
            columns.extend(column)
        elif column is not None:
            columns.append(column)

    # Create main table.
    main_table_name = get_pg_name(get_table_name(model))
    pkey_type = commands.get_primary_key_type(context, backend)
    main_table = sa.Table(
        main_table_name, backend.schema,
        sa.Column('_txn', pkey_type, index=True),
        sa.Column('_created', sa.DateTime),
        sa.Column('_updated', sa.DateTime),
        *columns,
    )
    backend.add_table(main_table, model)

    # Create changes table.
    changelog_table = get_changes_table(context, backend, model)
    backend.add_table(changelog_table, model, TableType.CHANGELOG)


@commands.prepare.register()
def prepare(context: Context, backend: PostgreSQL, dtype: DataType):
    if dtype.name in UNSUPPORTED_TYPES:
        return

    prop = dtype.prop
    name = get_column_name(prop)
    types = {
        'string': sa.Text,
        'date': sa.Date,
        'datetime': sa.DateTime,
        'integer': sa.Integer,
        'number': sa.Float,
        'boolean': sa.Boolean,
        'binary': sa.LargeBinary,
        'json': JSONB,
        'spatial': sa.Text,  # unsupported
        'image': sa.Text,  # unsupported
    }

    try:
        return sa.Column(name, types[dtype.name], unique=dtype.unique)
    except KeyError:
        raise Exception(
            f"Unknown type {dtype.name!r} for property {prop.place!r}."
        )


@commands.get_primary_key_type.register()
def get_primary_key_type(context: Context, backend: PostgreSQL):
    return UUID()


@commands.prepare.register()
def prepare(context: Context, backend: PostgreSQL, dtype: PrimaryKey):
    pkey_type = commands.get_primary_key_type(context, backend)
    return sa.Column('_id', pkey_type, primary_key=True)


@commands.prepare.register()
def prepare(context: Context, backend: PostgreSQL, dtype: Ref):
    # TODO: rename dtype.object to dtype.model
    ref_model = commands.get_referenced_model(context, dtype.prop, dtype.object)
    pkey_type = commands.get_primary_key_type(context, backend)
    return get_pg_foreign_key(
        dtype.prop,
        table_name=get_pg_name(get_table_name(ref_model)),
        model_name=dtype.prop.model.name,
        column_type=pkey_type,
    )


def get_pg_foreign_key(
    prop: Property,
    *,
    table_name: str,
    model_name: str,
    column_type: TypeEngine,
) -> List[Union[sa.Column, sa.Constraint]]:
    column_name = get_column_name(prop) + '._id'
    return [
        sa.Column(column_name, column_type),
        sa.ForeignKeyConstraint(
            [column_name], [f'{table_name}._id'],
            name=get_pg_name(f'fk_{model_name}_{column_name}'),
        )
    ]


@commands.prepare.register()
def prepare(context: Context, backend: PostgreSQL, dtype: File):
    prop = dtype.prop
    model = prop.model

    # All properties receive model backend even if properties own backend
    # differs. Its properties responsibility to deal with foreign backends.
    assert model.backend.name == backend.name

    pkey_type = commands.get_primary_key_type(context, model.backend)

    same_backend = backend.name == dtype.backend.name

    if same_backend:
        # If dtype is on the same backend currently being prepared, then
        # create table for storing file blocks and also add file metadata
        # columns.
        table_name = get_pg_name(get_table_name(prop, TableType.FILE))
        table = sa.Table(
            table_name, model.backend.schema,
            sa.Column('_id', pkey_type, primary_key=True),
            sa.Column('_block', sa.LargeBinary),
        )
        model.backend.add_table(table, prop, TableType.FILE)

    name = get_column_name(prop)

    # Required file metadata on any backend.
    columns = [
        sa.Column(f'{name}._id', sa.String),
        sa.Column(f'{name}._content_type', sa.String),
        sa.Column(f'{name}._size', BIGINT),
    ]

    # Optional file metadata, depending on backend supported features.
    if same_backend or BackendFeatures.FILE_BLOCKS in dtype.backend.features:
        columns += [
            sa.Column(f'{name}._bsize', sa.Integer),
            sa.Column(f'{name}._blocks', ARRAY(pkey_type)),
        ]

    return columns


@commands.prepare.register()
def prepare(context: Context, backend: PostgreSQL, dtype: Array):
    prop = dtype.prop

    columns = commands.prepare(context, backend, dtype.items)
    assert columns is not None
    if not isinstance(columns, list):
        columns = [columns]

    pkey_type = commands.get_primary_key_type(context, backend)

    # TODO: When all list items will have unique id, also add reference to
    #       parent list id.
    # if prop.list:
    #     parent_list_table_name = get_pg_name(
    #         get_table_name(prop.list, TableType.LIST)
    #     )
    #     columns += [
    #         # Parent list table id.
    #         sa.Column('_lid', pkey_type, sa.ForeignKey(
    #             f'{parent_list_table_name}._id', ondelete='CASCADE',
    #         )),
    #     ]

    name = get_pg_name(get_table_name(prop, TableType.LIST))
    main_table_name = get_pg_name(get_table_name(prop.model))
    table = sa.Table(
        name, backend.schema,
        # TODO: List tables eventually will have _id in order to uniquelly
        #       identify list item.
        # sa.Column('_id', pkey_type, primary_key=True),
        sa.Column('_txn', pkey_type, index=True),
        # Main table id (resource id).
        sa.Column('_rid', pkey_type, sa.ForeignKey(
            f'{main_table_name}._id', ondelete='CASCADE',
        )),
        *columns,
    )
    backend.add_table(table, prop, TableType.LIST)

    if prop.list is None:
        # For fast whole resource access we also store whole list in a JSONB.
        return sa.Column(prop.place, JSONB)


@commands.prepare.register()
def prepare(context: Context, backend: PostgreSQL, dtype: Object):
    columns = []
    for prop in dtype.properties.values():
        if prop.name.startswith('_') and prop.name not in ('_revision',):
            continue
        column = commands.prepare(context, backend, prop)
        if isinstance(column, list):
            columns.extend(column)
        elif column is not None:
            columns.append(column)
    return columns
