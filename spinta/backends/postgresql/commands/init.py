from typing import overload

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, UUID

from spinta import commands
from spinta.backends.constants import TableType
from spinta.backends.helpers import get_table_name
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.constants import UNSUPPORTED_TYPES
from spinta.backends.postgresql.helpers import get_column_name
from spinta.backends.postgresql.helpers import get_pg_name
from spinta.backends.postgresql.helpers.changes import get_changes_table
from spinta.components import Context, Model
from spinta.datasets.enums import Level
from spinta.manifests.components import Manifest
from spinta.types.datatype import DataType, PrimaryKey, Ref


@overload
@commands.prepare.register(Context, PostgreSQL, Manifest)
def prepare(context: Context, backend: PostgreSQL, manifest: Manifest, **kwargs):
    # Prepare backend for models.
    for model in commands.get_models(context, manifest).values():
        if model.backend and model.backend.name == backend.name:
            commands.prepare(context, backend, model, **kwargs)


@overload
@commands.prepare.register(Context, PostgreSQL, Model)
def prepare(context: Context, backend: PostgreSQL, model: Model, ignore_duplicate: bool = False, **kwargs):
    table_name = get_table_name(model)
    main_table_name = get_pg_name(table_name)
    if table_name in backend.tables and ignore_duplicate:
        return

    columns = []
    for prop in model.properties.values():
        # FIXME: _revision should has its own type and on database column type
        #        should bet received from get_primary_key_type() command.
        if prop.name.startswith('_') and prop.name not in ('_id', '_revision'):
            continue

        column = commands.prepare(context, backend, prop, **kwargs)

        if isinstance(column, list):
            columns.extend(column)
        elif column is not None:
            columns.append(column)

    if model.unique:
        for constraint in model.unique:
            prop_list = []
            for prop in constraint:
                name = prop.name
                if isinstance(prop.dtype, Ref):
                    if prop.level is None or prop.level > Level.open:
                        name = f'{name}._id'
                    elif prop.dtype:
                        name = f'{name}.{prop.dtype.refprops[0].name}'
                prop_list.append(name)

            columns.append(sa.UniqueConstraint(*prop_list))

    # Create main table.
    pkey_type = commands.get_primary_key_type(context, backend)
    main_table = sa.Table(
        main_table_name, backend.schema,
        sa.Column('_txn', pkey_type, index=True),
        sa.Column('_created', sa.DateTime),
        sa.Column('_updated', sa.DateTime),
        *columns,
    )
    if main_table_name == 'country':
        pp(model.manifest.path)
    backend.add_table(main_table, model)
    # Create changes table.
    changelog_table = get_changes_table(context, backend, model)
    backend.add_table(changelog_table, model, TableType.CHANGELOG)


@overload
@commands.prepare.register(Context, PostgreSQL, DataType)
def prepare(context: Context, backend: PostgreSQL, dtype: DataType, **kwargs):
    if dtype.name in UNSUPPORTED_TYPES:
        return
    prop = dtype.prop
    name = get_column_name(prop)
    types = {
        'string': sa.Text,
        'date': sa.Date,
        'time': sa.Time,
        'datetime': sa.DateTime,
        'temporal': sa.DateTime,
        'integer': sa.Integer,
        'number': sa.Float,
        'boolean': sa.Boolean,
        'binary': sa.LargeBinary,
        'json': JSONB,
        'spatial': sa.Text,  # unsupported
        'image': sa.Text,  # unsupported
        'url': sa.String,
        'uri': sa.String,
        'denorm': sa.String,
    }

    if dtype.name not in types:
        raise Exception(
            f"Unknown type {dtype.name!r} for property {prop.place!r}."
        )
    column_type = types[dtype.name]
    nullable = not dtype.required
    return sa.Column(name, column_type, unique=dtype.unique, nullable=nullable)


@commands.get_primary_key_type.register()
def get_primary_key_type(context: Context, backend: PostgreSQL):
    return UUID()


@overload
@commands.prepare.register(Context, PostgreSQL, PrimaryKey)
def prepare(context: Context, backend: PostgreSQL, dtype: PrimaryKey, **kwargs):
    pkey_type = commands.get_primary_key_type(context, backend)
    base = dtype.prop.model.base
    if base and (base.level and base.level >= Level.identifiable or not base.level):
        return [
            sa.Column('_id', pkey_type, primary_key=True),
            sa.ForeignKeyConstraint(
                ['_id'], [f'{get_pg_name(get_table_name(base.parent))}._id'],
                name=get_pg_name(f'fk_{base.parent.name}_id'),
            )
        ]
    return sa.Column('_id', pkey_type, primary_key=True)
