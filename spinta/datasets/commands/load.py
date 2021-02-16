from typing import List

from spinta import commands
from spinta.core.ufuncs import asttoexpr
from spinta.datasets.components import Attribute
from spinta.datasets.helpers import load_resource_backend
from spinta.nodes import get_node, load_node
from spinta.components import Context
from spinta.manifests.components import Manifest
from spinta.datasets.components import Dataset, Resource, Entity
from spinta.core.access import load_access_param
from spinta.types.namespace import load_namespace_from_name
from spinta.utils.data import take


@commands.load.register(Context, Dataset, dict, Manifest)
def load(
    context: Context,
    dataset: Dataset,
    data: dict,
    manifest: Manifest,
    *,
    source: Manifest = None,
):
    config = context.get('config')

    load_node(context, dataset, data, parent=manifest)
    load_access_param(dataset, data.get('access'), (manifest,))

    ns = load_namespace_from_name(context, manifest, dataset.name, drop=False)
    if ns.generated:
        ns.title = dataset.title
        ns.description = dataset.description

    # Load resources
    dataset.resources = {}
    for name, params in (data.get('resources') or {}).items():
        resource = get_node(config, manifest, dataset.eid, data, parent=dataset, group='datasets', ctype='resource')
        resource.type = params.get('type')
        resource.name = name
        resource.dataset = dataset
        dataset.resources[name] = load(context, resource, params, manifest)

    return dataset


@commands.load.register(Context, Resource, dict, Manifest)
def load(context: Context, resource: Resource, data: dict, manifest: Manifest):
    load_node(context, resource, data, parent=resource.dataset)
    if resource.prepare:
        resource.prepare = asttoexpr(resource.prepare)
    load_access_param(resource, data.get('access'), (resource.dataset,))
    resource.backend = load_resource_backend(
        context,
        resource,
        # First backend is loaded as string and later becomes Backend.
        resource.backend,
    )

    # Models will be added on `link` command.
    resource.models = {}
    return resource


@commands.load.register(Context, Entity, dict, Manifest)
def load(context: Context, entity: Entity, data: dict, manifest: Manifest):
    # XXX: https://gitlab.com/atviriduomenys/spinta/-/issues/44
    pkeys: List[str] = entity.pkeys or []
    if pkeys:
        entity.pkeys = [entity.model.properties[k] for k in pkeys]
    else:
        entity.unknown_primary_key = True
        entity.pkeys = sorted(
            take(entity.model.properties).values(),
            key=lambda p: p.place,
        )

    if entity.prepare:
        entity.prepare = asttoexpr(entity.prepare)
    return entity


@commands.load.register(Context, Attribute, dict, Manifest)
def load(context: Context, attr: Attribute, data: dict, manifest: Manifest):
    if attr.prepare:
        attr.prepare = asttoexpr(attr.prepare)
    return attr
