from spinta import commands
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.constants import TableType
from spinta.backends.helpers import get_table_name
from spinta.backends.postgresql.helpers import get_column_name
from spinta.backends.postgresql.helpers import get_pg_name
from spinta.components import Context, Model
from spinta.types.datatype import DataType
from spinta.migrations import SchemaVersion
from spinta.utils.data import take


@commands.freeze.register(Context, SchemaVersion, PostgreSQL, type(None), Model)
def freeze(
    context: Context,
    version: SchemaVersion,
    backend: PostgreSQL,
    freezed: type(None),
    current: Model,
):
    props = []
    for prop in take(['_id', '_revision', all], current.properties).values():
        props.extend(
            freeze(context, version, backend, freezed, prop.dtype)
        )
    version.actions.append({
        'type': 'schema',
        'upgrade': {
            'name': 'create_table',
            'args': [current.name] + props,
        },
        'downgrade': {
            'name': 'drop_table',
            'args': [current.name],
        },
    })


def dzip(*data):
    keys = set()
    for d in data:
        keys.update(d)
    for key in keys:
        yield tuple(d.get(key) for d in data)


@commands.freeze.register(Context, SchemaVersion, PostgreSQL, Model, Model)
def freeze(
    context: Context,
    version: SchemaVersion,
    backend: PostgreSQL,
    freezed: Model,
    current: Model,
):
    for freezed_prop, current_prop in dzip(freezed.properties, current.properties):
        freeze(
            context,
            version,
            backend,
            freezed_prop.dtype if freezed_prop else freezed,
            current_prop.dtype if current_prop else current,
        )


@commands.freeze.register(Context, SchemaVersion, PostgreSQL, type(None), DataType)
def freeze(
    context: Context,
    version: SchemaVersion,
    backend: PostgreSQL,
    freezed: type(None),
    current: DataType,
):
    column = {
        'name': 'column',
        'args': [
            current.prop.place,
            {
                'name': current.name,
                'args': [],
            }
        ]
    }
    if current.nullable:
        column['args'].append({'name': 'bind', 'args': ['nullable', True]})
    return [column]


@commands.freeze.register(Context, SchemaVersion, PostgreSQL, Model, DataType)
def freeze(
    context: Context,
    version: SchemaVersion,
    backend: PostgreSQL,
    freezed: Model,
    current: DataType,
):
    column = {
        'name': 'column',
        'args': [
            get_column_name(current.prop),
            {'name': current.name, 'args': []}
        ]
    }
    if current.nullable:
        column['args'].append({'name': 'bind', 'args': ['nullable', True]})

    upgrade = {
        'name': 'add_column',
        'args': [
            get_table_name(current.prop),
            column,
        ]
    }

    version.actions.append({
        'type': 'schema',
        'upgrade': upgrade,
        'downgrade': {
            'name': 'drop_column',
            'args': [
                get_table_name(current.prop),
                get_column_name(current.prop),
            ]
        }
    })


@commands.freeze.register(Context, SchemaVersion, PostgreSQL, DataType, Model)
def freeze(
    context: Context,
    version: SchemaVersion,
    backend: PostgreSQL,
    freezed: DataType,
    current: Model,
):
    pass


@commands.freeze.register(Context, SchemaVersion, PostgreSQL, DataType, DataType)
def freeze(
    context: Context,
    version: SchemaVersion,
    backend: PostgreSQL,
    freezed: DataType,
    current: DataType,
):
    table_name = get_table_name(current.prop)
    if current.prop.list:
        table_name = get_pg_name(get_table_name(current.prop, TableType.LIST))
    if freezed.name != current.name:
        actions = {
            'type': 'schema',
            'upgrade': {
                'name': 'alter_column',
                'args': [
                    table_name,
                    get_column_name(current.prop),
                    {
                        'name': 'bind',
                        'args': [
                            'type_',
                            {'name': current.name, 'args': []}
                        ]
                    },
                ]
            },
            'downgrade': {
                'name': 'alter_column',
                'args': [
                    table_name,
                    get_column_name(current.prop),
                    {
                        'name': 'bind',
                        'args': [
                            'type_',
                            {'name': freezed.name, 'args': []}
                        ]
                    },
                ]
            }
        }
        if freezed.nullable != current.nullable:
            actions['upgrade']['args'].append(
                {'name': 'bind', 'args': ['nullable', current.nullable]}
            )
            actions['downgrade']['args'].append(
                {'name': 'bind', 'args': ['nullable', freezed.nullable]}
            )
        version.actions.append(actions)
    elif freezed.nullable != current.nullable:
        version.actions.append({
            'type': 'schema',
            'upgrade': {
                'name': 'alter_column',
                'args': [
                    table_name,
                    get_column_name(current.prop),
                    {'name': 'bind', 'args': ['nullable', current.nullable]},
                ]
            },
            'downgrade': {
                'name': 'alter_column',
                'args': [
                    table_name,
                    get_column_name(current.prop),
                    {'name': 'bind', 'args': ['nullable', freezed.nullable]},
                ]
            }
        })
