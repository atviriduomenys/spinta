from spinta import commands
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.constants import TableType
from spinta.backends.postgresql.helpers import get_column_name, get_table_name, \
    get_pg_name
from spinta.components import Context, Model
from spinta.types.datatype import DataType, Object, Array
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
            freeze(context, version, backend, prop.dtype)
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
            freezed_prop.dtype if freezed_prop else None,
            current_prop.dtype if current_prop else None,
        )


@commands.freeze.register(Context, SchemaVersion, PostgreSQL, DataType)
def freeze(
    context: Context,
    version: SchemaVersion,
    backend: PostgreSQL,
    current: DataType,
):
    return [
        {
            'name': 'column',
            'args': [
                current.prop.place,
                {
                    'name': current.name,
                    'args': [],
                },
            ]
        },
    ]


@commands.freeze.register(Context, SchemaVersion, PostgreSQL, type(None), Object)
def freeze(
    context: Context,
    version: SchemaVersion,
    backend: PostgreSQL,
    freezed: type(None),
    current: Object,
):
    for name, prop in current.properties.items():
        version.actions.append({
            'type': 'schema',
            'upgrade': {
                'name': 'add_column',
                'args': [
                    get_table_name(prop),
                    get_column_name(prop),
                    {'name': prop.dtype.name, 'args': []},
                ]
            },
            'downgrade': {
                'name': 'drop_column',
                'args': [
                    get_table_name(prop),
                    get_column_name(prop),
                ]
            }
        })


@commands.freeze.register(Context, SchemaVersion, PostgreSQL, type(None), DataType)
def freeze(
    context: Context,
    version: SchemaVersion,
    backend: PostgreSQL,
    freezed: type(None),
    current: DataType,
):
    version.actions.append({
        'type': 'schema',
        'upgrade': {
            'name': 'add_column',
            'args': [
                get_table_name(current.prop),
                get_column_name(current.prop),
                {'name': current.name, 'args': []},
            ]
        },
        'downgrade': {
            'name': 'drop_column',
            'args': [
                get_table_name(current.prop),
                get_column_name(current.prop),
            ]
        }
    })


@commands.freeze.register(Context, SchemaVersion, PostgreSQL, Object, Object)
def freeze(
    context: Context,
    version: SchemaVersion,
    backend: PostgreSQL,
    freezed: Object,
    current: Object,
):
    for name, prop in current.properties.items():
        if name in freezed.properties:
            commands.freeze(context, version, backend, freezed.properties[name].dtype, prop.dtype)
        else:
            commands.freeze(context, version, backend, None, prop.dtype)


@commands.freeze.register(Context, SchemaVersion, PostgreSQL, Array, Array)
def freeze(
    context: Context,
    version: SchemaVersion,
    backend: PostgreSQL,
    freezed: Array,
    current: Array,
):
    if current.items.dtype.name != freezed.items.dtype.name:
        list_table_name = get_pg_name(get_table_name(current.prop, TableType.LIST))
        version.actions.append({
            'type': 'schema',
            'upgrade': {
                'name': 'alter_column',
                'args': [
                    list_table_name,
                    get_column_name(current.prop),
                    {'name': current.items.dtype.name, 'args': []},
                ]
            },
            'downgrade': {
                'name': 'alter_column',
                'args': [
                    list_table_name,
                    get_column_name(current.prop),
                    {'name': freezed.items.dtype.name, 'args': []},
                ]
            }
        })


@commands.freeze.register(Context, SchemaVersion, PostgreSQL, DataType, DataType)
def freeze(  # noqa
    context: Context,
    version: SchemaVersion,
    backend: PostgreSQL,
    freezed: DataType,
    current: DataType,
):
    if freezed.name != current.name:
        version.actions.append({
            'type': 'schema',
            'upgrade': {
                'name': 'alter_column',
                'args': [
                    get_table_name(current.prop),
                    get_column_name(current.prop),
                    {'name': current.name, 'args': []},
                ]
            },
            'downgrade': {
                'name': 'alter_column',
                'args': [
                    get_table_name(current.prop),
                    get_column_name(current.prop),
                    {'name': freezed.name, 'args': []},
                ]
            }
        })
