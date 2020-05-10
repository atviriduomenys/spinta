from spinta import commands
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
    resource.access = load_access_param(resource, resource.access)

    store = resource.dataset.manifest.store
    possible_backends = [
        resource.backend,
        f'{resource.dataset.name}/{resource.name}',
        f'{resource.dataset.name}',
        f'{resource.type}',
    ]
    possible_backends = [pb.replace('/', '_') for pb in possible_backends if pb]
    for backend in possible_backends:
        if backend in store.backends:
            resource.backend = store.backends[backend]
            break
    else:
        resource.backend = manifest.backend

    # Models will be added on `link` command.
    resource.models = {}
    return resource


@commands.load.register(Context, Entity, dict, Manifest)
def load(context: Context, entity: Entity, data: dict, manifest: Manifest):
    return entity
