from spinta import commands
from spinta.components import Context
from spinta.datasets.components import Dataset, Resource, Entity, Attribute
from spinta.exceptions import MissingReferrence


@commands.link.register(Context, Dataset)
def link(context: Context, dataset: Dataset):
    for resource in dataset.resources.values():
        commands.link(context, resource)


@commands.link.register(Context, Resource)
def link(context: Context, resource: Resource):
    pass


@commands.link.register(Context, Entity)
def link(context: Context, entity: Entity):
    datasets = entity.model.manifest.objects['dataset']
    if entity.dataset not in datasets:
        raise MissingReferrence(entity, param='dataset', ref=entity.dataset)
    entity.dataset = datasets[entity.dataset]

    resources = entity.dataset.resources
    if entity.resource not in resources:
        raise MissingReferrence(entity, param='resource', ref=entity.resource)
    entity.resource = resources[entity.resource]

    entity.pkeys = [entity.model.properties[p] for p in (entity.pkeys or [])]

    assert entity.model.name not in entity.resource.models
    entity.resource.models[entity.model.name] = entity.model


@commands.link.register(Context, Attribute)
def link(context: Context, attribute: Attribute):
    pass
