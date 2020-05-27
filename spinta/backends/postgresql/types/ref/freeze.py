from spinta import commands
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers import get_column_name, get_table_name
from spinta.components import Context
from spinta.types.datatype import Ref
from spinta.migrations import SchemaVersion


@commands.freeze.register(Context, SchemaVersion, PostgreSQL, type(None), Ref)
def freeze(
    context: Context,
    version: SchemaVersion,
    backend: PostgreSQL,
    freezed: type(None),
    current: Ref,
):
    return [
        {
            'name': 'column',
            'args': [
                f'{get_column_name(current.prop)}._id',
                {
                    'name': 'ref',
                    'args': [f'{get_table_name(current.model)}._id'],
                },
            ]
        },
    ]


@commands.freeze.register(Context, SchemaVersion, PostgreSQL, Ref, Ref)
def freeze(
    context: Context,
    version: SchemaVersion,
    backend: PostgreSQL,
    freezed: Ref,
    current: Ref,
):
    if freezed.model.name != current.model.name:
        version.actions.append({
            'type': 'schema',
            'upgrade': {
                'name': 'alter_column',
                'args': [
                    get_table_name(current.prop),
                    f'{get_column_name(current.prop)}._id',
                    {
                        'name': 'ref',
                        'args': [f'{get_table_name(current.model)}._id'],
                    }
                ]
            },
            'downgrade': {
                'name': 'alter_column',
                'args': [
                    get_table_name(freezed.prop),
                    f'{get_column_name(freezed.prop)}._id',
                    {
                        'name': 'ref',
                        'args': [f'{get_table_name(freezed.model)}._id'],
                    },
                ]
            }
        })
