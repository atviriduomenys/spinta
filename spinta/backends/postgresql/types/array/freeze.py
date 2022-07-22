from spinta import commands
from spinta.components import Context
from spinta.types.datatype import Array
from spinta.migrations import SchemaVersion
from spinta.backends.constants import TableType
from spinta.backends.helpers import get_table_name
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers import get_pg_name


@commands.freeze.register(Context, SchemaVersion, PostgreSQL, type(None), Array)
def freeze(
    context: Context,
    version: SchemaVersion,
    backend: PostgreSQL,
    freezed: type(None),
    current: Array,
):
    list_table_name = get_pg_name(get_table_name(current.prop, TableType.LIST))
    main_table_name = get_pg_name(get_table_name(current.prop.model))

    props = [
        {
            'name': 'column',
            'args': ['_txn', {'name': 'uuid', 'args': []}],
        },
        {
            'name': 'column',
            'args': ['_rid', {'name': 'ref', 'args': [
                f'{main_table_name}._id', {
                    'name': 'bind',
                    'args': ['ondelete', 'CASCADE'],
                }
            ]}],
        },
    ]
    props += commands.freeze(context, version, backend, None, current.items.dtype)
    version.actions.append({
        'type': 'schema',
        'upgrade': {
            'name': 'create_table',
            'args': [list_table_name] + props,
        },
        'downgrade': {
            'name': 'drop_table',
            'args': [list_table_name],
        },
    })

    if current.prop.list is None:
        # For fast whole resource access we also store whole list in a JSONB.
        return [
            {
                'name': 'column',
                'args': [
                    current.prop.place,
                    {
                        'name': 'json',
                        'args': [],
                    },
                ]
            },
        ]

    else:
        return []


@commands.freeze.register(Context, SchemaVersion, PostgreSQL, Array, Array)
def freeze(
    context: Context,
    version: SchemaVersion,
    backend: PostgreSQL,
    freezed: Array,
    current: Array,
):
    commands.freeze(context, version, backend, freezed.items.dtype, current.items.dtype)
