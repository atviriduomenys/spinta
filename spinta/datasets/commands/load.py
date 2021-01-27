from spinta import commands
from spinta.core.ufuncs import asttoexpr
from spinta.datasets.components import Attribute
from spinta.datasets.helpers import load_resource_backend
from spinta.nodes import get_node, load_node
from spinta.components import Context
from spinta.manifests.components import Manifest
from spinta.datasets.components import Dataset, Resource, Entity
from spinta.core.access import load_access_param


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
    dataset.access = load_access_param(dataset, dataset.access)

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
    resource.access = load_access_param(resource, resource.access)
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
    if entity.prepare:
        entity.prepare = asttoexpr(entity.prepare)
    return entity


@commands.load.register(Context, Attribute, dict, Manifest)
def load(context: Context, attr: Attribute, data: dict, manifest: Manifest):
    if attr.prepare:
        attr.prepare = asttoexpr(attr.prepare)
    return attr
