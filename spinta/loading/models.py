from typing import Dict

from spinta import commands
from spinta.core.context import Context
from spinta.core.component import create_component
from spinta.components.model import Model, Property
from spinta.components.core import Namespace


@commands.load.register()
def load(context: Context, model: Model) -> Model:
    manifest = model.parent
    _add_model_endpoint(model)
    model.ns = _load_model_namespace(context, model)
    model.external = _load_model_external_source(context, manifest, model)
    model.properties = _load_model_properties(context, model)
    return model


def _add_model_endpoint(model: Model):
    manifest = model.parent
    endpoint = model.endpoint
    if endpoint:
        if endpoint not in manifest.endpoints:
            manifest.endpoints[endpoint] = model.name
        elif manifest.endpoints[endpoint] != model.name:
            raise Exception(f"Same endpoint, but different model name, endpoint={endpoint!r}, model.name={model.name!r}.")


def _load_model_namespace(context: Context, model: Model):
    parts = []
    manifest = model.parent
    for part in [''] + model.name.split('/'):
        parts.append(part)
        name = '/'.join(parts[1:])
        if name not in manifest.nodes['ns']:
            ns = Namespace()
            data = {
                'type': 'ns',
                'name': name,
                'title': part,
                'path': manifest.path,
                'parent': parent,
                'names': {},
                'models': {},
                'backend': None,
            }
            manifest.objects['ns'][name] = load_node(context, ns, data, manifest)
        else:
            ns = manifest.objects['ns'][name]
        if part and part not in parent.names:
            parent.names[part] = ns
        parent = ns
    parent.models[node.model_type()] = node


def _load_model_external_source(
    context: Context,
    model: Model,
) -> Source:
    if model.external is not None:
        manifest = model.parent
        data = model.external
        dataset = data['dataset']
        dataset = manifest.objects['dataset'][dataset]
        resource = data['resource']
        resource = dataset.resources[resource]
        external = create_component(context, model, data, resource.type)
        external.dataset = dataset
        external.resource = resource
        commands.load(context, external)
        return external


def _load_model_properties(context: Context, model: Model) -> Dict[str, Property]:
    data = model.properties or {}

    # Add build-in properties.
    data = {
        '_op': {'type': 'string'},
        '_type': {'type': 'string'},
        '_id': {'type': 'pk', 'unique': True},
        '_revision': {'type': 'string'},
        '_transaction': {'type': 'integer'},
        '_where': {'type': 'rql'},
        **data,
    }

    model.flatprops = {}
    model.leafprops = {}
    model.properties = {}
    for name, params in data.items():
        prop, dtype = create_component(
            context,
            model,
            params,
            ctype='property',
            group='nodes',
            mixed=True,
            inherit=model,
        )
        prop.dtype = dtype
        prop.name = name
        prop.place = name
        prop.model = model
        model.properties[name] = prop
        model.flatprops[name] = prop
        commands.load(context, prop)


@commands.load.register()
def load(context: Context, prop: Property) -> Property:
    config = context.get('config')

    prop.dtype = create_component(
        config,
        prop,
        prop.dtype,
        group='types',
    )
    commands.load(context, prop.dtype)

    prop.external = create_component(
        context,
        prop,
        prop.external,
        ctype='source',
        group='core',
        inherit=prop.model.external,
    )
    commands.load(context, prop.external)

    return prop


@commands.check.register()
def check(context: Context, model: Model) -> None:
    pass
