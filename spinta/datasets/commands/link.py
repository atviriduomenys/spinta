from typing import List

from spinta import commands
from spinta.components import Context
from spinta.core.access import link_access_param
from spinta.datasets.components import Dataset, Resource, Entity, Attribute
from spinta.dimensions.param.helpers import link_params
from spinta.exceptions import MissingReference
from spinta.types.datatype import Partial, Ref
from spinta.components import Property


@commands.link.register(Context, Dataset)
def link(context: Context, dataset: Dataset):
    link_access_param(dataset, (dataset.manifest,))
    for resource in dataset.resources.values():
        commands.link(context, resource)


@commands.link.register(Context, Resource)
def link(context: Context, resource: Resource):
    link_access_param(resource, (resource.dataset,))
    if resource.params and resource.manifest:
        link_params(context, resource.manifest, resource.params, resource.dataset)


@commands.link.register(Context, Entity)
def link(context: Context, entity: Entity):
    manifest = entity.model.manifest
    if entity.dataset:
        if not commands.has_dataset(context, manifest, entity.dataset):
            raise MissingReference(
                entity,
                param='dataset',
                ref=entity.dataset,
            )
        # XXX: https://gitlab.com/atviriduomenys/spinta/-/issues/44
        dataset: str = entity.dataset
        entity.dataset = commands.get_dataset(context, manifest, dataset)

        resources = entity.dataset.resources
        if entity.resource:
            if entity.resource not in resources:
                raise MissingReference(
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

    link_params(context, manifest, entity.params, entity.dataset)


@commands.link.register(Context, Attribute)
def link(context: Context, attribute: Attribute):
    pass
