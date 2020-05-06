import itertools

from spinta import commands
from spinta.backends.postgresql.components import PostgreSQL
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
    props = list(itertools.chain(*[
        freeze(context, version, backend, None, prop.dtype)
        for prop in take(['_id', '_revision', all], current.properties).values()
    ]))
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
def freeze(  # noqa
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
def freeze(  # noqa
    context: Context,
    version: SchemaVersion,
    backend: PostgreSQL,
    freezed: type(None),
    current: DataType,
):
    return [
        {
            'name': 'column',
            'args': [
                current.prop.name,
                {
                    'name': current.name,
                    'args': [],
                },
            ]
        },
    ]


@commands.freeze.register(Context, SchemaVersion, PostgreSQL, Model, DataType)
def freeze(  # noqa
    context: Context,
    version: SchemaVersion,
    backend: PostgreSQL,
    freezed: Model,
    current: DataType,
):
    version.actions.append({
        'type': 'schema',
        'upgrade': {
            'name': 'add_column',
            'args': [
                current.prop.model.name,
                current.prop.name,
                {'name': current.name, 'args': []},
            ]
        },
        'downgrade': {
            'name': 'drop_column',
            'args': [
                current.prop.model.name,
                current.prop.name,
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
                    current.prop.model.name,
                    current.prop.name,
                    {'name': current.name, 'args': []},
                ]
            },
            'downgrade': {
                'name': 'alter_column',
                'args': [
                    current.prop.model.name,
                    current.prop.name,
                    {'name': freezed.name, 'args': []},
                ]
            }
        })
