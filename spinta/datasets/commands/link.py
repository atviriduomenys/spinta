from typing import List

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
    if entity.dataset:
        if entity.dataset not in datasets:
            raise MissingReferrence(
                entity,
                param='dataset',
                ref=entity.dataset,
            )
        # XXX: https://gitlab.com/atviriduomenys/spinta/-/issues/44
        dataset: str = entity.dataset
        entity.dataset = datasets[dataset]

        resources = entity.dataset.resources
        if entity.resource:
            if entity.resource not in resources:
                raise MissingReferrence(
                    entity,
                    param='resource',
                    ref=entity.resource,
                )
            # XXX: https://gitlab.com/atviriduomenys/spinta/-/issues/44
            resource: str = entity.resource
            entity.resource = resources[resource]

            assert entity.model.name not in entity.resource.models
            entity.resource.models[entity.model.name] = entity.model
        else:
            entity.resource = None
    else:
        entity.dataset = None

    # XXX: https://gitlab.com/atviriduomenys/spinta/-/issues/44
    pkeys: List[str] = entity.pkeys or []
    entity.pkeys = [entity.model.properties[k] for k in pkeys]


@commands.link.register(Context, Attribute)
def link(context: Context, attribute: Attribute):
    pass
