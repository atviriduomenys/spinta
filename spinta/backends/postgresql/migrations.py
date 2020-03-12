import itertools

from spinta import commands
from spinta.backends.postgresql import PostgreSQL
from spinta.components import Context, Model
from spinta.types.datatype import DataType
from spinta.migrations import SchemaVersion


@commands.freeze.register()
def freeze(
    context: Context,
    version: SchemaVersion,
    backend: PostgreSQL,
    old: type(None),
    new: Model,
):
    props = [
        {
            'name': 'column',
            'args': ['_id', {'name': 'pk', 'args': []}]
        },
        {
            'name': 'column',
            'args': ['_revision', {'name': 'string', 'args': []}]
        },
    ]
    props += itertools.chain(*[
        freeze(context, version, backend, None, prop.dtype)
        for prop in new.properties.values()
    ])
    version.actions.append({
        'type': 'schema',
        'upgrade': {
            'name': 'create_table',
            'args': [new.name] + props,
        },
        'downgrade': {
            'name': 'drop_table',
            'args': [new.name],
        },
    })


def merge(*data):
    keys = set()
    for d in data:
        keys.update(d)
    for key in keys:
        yield tuple(d.get(key) for d in data)


@commands.freeze.register()
def freeze(  # noqa
    context: Context,
    version: SchemaVersion,
    backend: PostgreSQL,
    old: Model,
    new: Model,
):
    for old_prop, new_prop in merge(old.properties, new.properties):
        freeze(
            context,
            version,
            backend,
            old_prop.dtype if old_prop else old,
            new_prop.dtype if new_prop else new,
        )


@commands.freeze.register()
def freeze(  # noqa
    context: Context,
    version: SchemaVersion,
    backend: PostgreSQL,
    old: type(None),
    new: DataType,
):
    return [
        {
            'name': 'column',
            'args': [
                new.prop.name,
                {
                    'name': new.name,
                    'args': [],
                },
            ]
        },
    ]


@commands.freeze.register()
def freeze(  # noqa
    context: Context,
    version: SchemaVersion,
    backend: PostgreSQL,
    old: Model,
    new: DataType,
):
    version.actions.append({
        'type': 'schema',
        'upgrade': {
            'name': 'add_column',
            'args': [
                new.model.name,
                new.prop.name,
                {'name': new.name, 'args': []},
            ]
        },
        'downgrade': {
            'name': 'drop_column',
            'args': [
                new.model.name,
                new.prop.name,
            ]
        }
    })


@commands.freeze.register()
def freeze(  # noqa
    context: Context,
    version: SchemaVersion,
    backend: PostgreSQL,
    old: DataType,
    new: DataType,
):
    if old.name != new.name:
        version.actions.append({
            'type': 'schema',
            'upgrade': {
                'name': 'alter_column',
                'args': [
                    new.model.name,
                    new.prop.name,
                    {'name': new.name, 'args': []},
                ]
            },
            'downgrade': {
                'name': 'alter_column',
                'args': [
                    new.model.name,
                    new.prop.name,
                    {'name': old.name, 'args': []},
                ]
            }
        })
