from spinta import commands
from spinta.components import Context
from spinta.core.access import link_access_param
from spinta.datasets.components import Attribute, Dataset, Entity, Resource
from spinta.dimensions.enum.helpers import load_enums
from spinta.dimensions.param.helpers import link_params
from spinta.exceptions import MissingReference
from spinta.types.namespace import ensure_dataset_namespace
from spinta.ufuncs.linkbuilder.components import LinkBuilder


@commands.link.register(Context, Dataset)
def link(context: Context, dataset: Dataset):
    config = context.get("config")

    # Make sure the dataset is wired to its namespace (build_namespaces already
    # did this for the full load path; the lazy internal_sql path relies on this
    # call). Then resolve namespace metadata and enums that used to be handled
    # during loading.
    ns = ensure_dataset_namespace(context, dataset.manifest, dataset)
    if ns.generated:
        ns.title = dataset.title
        ns.description = dataset.description
    if dataset.given.enums is not None:
        ns.enums = load_enums(context, list(ns.parents()), dataset.given.enums)

    link_access_param(dataset, (dataset.manifest,), default_access=config.default_access_level)
    for resource in dataset.resources.values():
        commands.link(context, resource)


@commands.link.register(Context, Resource)
def link(context: Context, resource: Resource):
    config = context.get("config")
    link_access_param(resource, (resource.dataset,), default_access=config.default_access_level)
    if resource.params and resource.manifest:
        link_params(context, resource.manifest, resource.params, resource.dataset)

    resource_builder = LinkBuilder(context, resource, resource.dataset)
    resource_builder.resolve(resource.prepare)


@commands.link.register(Context, Entity)
def link(context: Context, entity: Entity):
    manifest = entity.model.manifest
    if entity.dataset:
        if not commands.has_dataset(context, manifest, entity.dataset):
            raise MissingReference(
                entity,
                param="dataset",
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
                    param="resource",
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
