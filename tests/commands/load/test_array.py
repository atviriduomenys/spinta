import pathlib

from spinta.commands import load
from spinta.components import Model
from spinta.types.datatype import Object, Array


def create_model(context, schema):
    manifest = context.get('store').manifests['default']
    backend = context.get('store').backends['default']
    model = {
        'type': 'model',
        'name': 'model',
        'path': pathlib.Path('model.yml'),
        'parent': manifest,
        'backend': backend,
        **schema,
    }
    return load(context, Model(), model, manifest)


def show_lists(dtype, parent=()):
    if isinstance(dtype, Model) or dtype.prop.list is None:
        listname = None
    else:
        listname = dtype.prop.list.place
    if isinstance(dtype, (Model, Object)):
        if isinstance(dtype, Model):
            this = {}
        else:
            this = {'.'.join(parent): listname}
        return {
            **this,
            **{
                k: v
                for prop in dtype.properties.values()
                for k, v in show_lists(prop.dtype, parent + (prop.name,)).items()
            }
        }
    elif isinstance(dtype, Array):
        return {
            '.'.join(parent): listname,
            **show_lists(dtype.items.dtype, parent[:-1] + (parent[-1] + '[]',))
        }
    elif dtype.prop.name.startswith('_'):
        return {}
    else:
        return {'.'.join(parent): listname}


def test_array_refs(context):
    model = create_model(context, {
        'properties': {
            'scalar': {'type': 'string'},
            'list': {
                'type': 'array',
                'items': {
                    'type': 'object',
                    'properties': {
                        'foo': {'type': 'string'},
                        'bar': {
                            'type': 'array',
                            'items': {'type': 'string'}
                        },
                    },
                },
            },
        },
    })
    assert show_lists(model) == {
        'list': None,
        'list[]': 'list',
        'list[].bar': 'list',
        'list[].bar[]': 'list.bar',
        'list[].foo': 'list',
        'scalar': None,
    }
