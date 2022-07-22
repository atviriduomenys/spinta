from spinta import commands
from spinta.components import Context, Model
from spinta.backends.helpers import get_table_name
from spinta.types.datatype import Ref
from spinta.migrations import SchemaVersion
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers import get_column_name


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


@commands.freeze.register(Context, SchemaVersion, PostgreSQL, Model, Ref)
def freeze(
    context: Context,
    version: SchemaVersion,
    backend: PostgreSQL,
    freezed: Model,
    current: Ref,
):
    column = {
        'name': 'column',
        'args': [
            f'{get_column_name(current.prop)}._id',
            {
                'name': 'ref',
                'args': [f'{get_table_name(current.model)}._id'],
            }
        ]
    }

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
                f'{get_column_name(current.prop)}._id',
            ]
        }
    })


@commands.freeze.register(Context, SchemaVersion, PostgreSQL, Ref, Ref)
def freeze(
    context: Context,
    version: SchemaVersion,
    backend: PostgreSQL,
    freezed: Ref,
    current: Ref,
):
    if freezed.model.name != current.model.name:
        raise NotImplementedError("Could not change model's reference")
