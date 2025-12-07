from typer import echo

from spinta import commands
from spinta.cli.helpers.sync.controllers.dsa import get_dsa
from spinta.cli.helpers.sync.controllers.synchronization import (
    FIELDS_TO_MERGE_FOR_DATASET,
    FIELDS_TO_MERGE_FOR_RESOURCE,
    FIELDS_TO_MERGE_FOR_MODEL,
    FIELDS_TO_MERGE_FOR_PROPERTY,
)
from spinta.cli.helpers.sync.helpers import merge_manifest_attributes, find_existing_entity, read_and_get_manifest
from spinta.components import Context, Model
from spinta.core.enums import Access
from spinta.datasets.components import Dataset, Resource
from spinta.manifests.components import Manifest
from spinta.manifests.helpers import init_manifest
from spinta.manifests.tabular.helpers import write_tabular_manifest, datasets_to_tabular


PRIVATE_PROPERTY_PREFIX = "_"


def execute_synchronization_catalog_to_agent(
    context: Context,
    base_path: str,
    headers: dict[str, str],
    manifest_path: str,
    dataset_ids: list[str],
) -> Manifest:
    manifest = Manifest()
    init_manifest(context, manifest, "sync")

    if not dataset_ids:
        return manifest

    local_manifest = read_and_get_manifest(context, path=manifest_path)
    catalog_manifest_contents = [get_dsa(base_path, headers, dataset_id) for dataset_id in dataset_ids]
    if not catalog_manifest_contents:
        return manifest
    catalog_manifest = read_and_get_manifest(context, contents=catalog_manifest_contents)

    catalog_tabular = list(datasets_to_tabular(context, catalog_manifest, external=True, access=Access.private))
    local_tabular = list(datasets_to_tabular(context, local_manifest, external=True, access=Access.private))

    if catalog_tabular == local_tabular:
        echo("Catalog and local manifests are identical.")
        return local_manifest

    if not local_manifest or not local_tabular:
        echo("Local manifest is empty, catalog manifest will be loaded locally.")
        write_tabular_manifest(context, manifest_path, catalog_manifest)
        return catalog_manifest

    echo("Local manifest differs from catalog manifest. Catalog fields will take precedence.")
    merge_manifest_datasets(context, local_manifest, catalog_manifest)
    write_tabular_manifest(context, manifest_path, local_manifest)
    return local_manifest


def merge_manifest_datasets(context: Context, internal_manifest: Manifest, external_manifest: Manifest) -> None:
    external_datasets = commands.get_datasets(context, external_manifest)
    internal_datasets = commands.get_datasets(context, internal_manifest)
    for dataset_name, dataset_object in external_datasets.items():
        existing_dataset, existing_dataset_name = find_existing_entity(dataset_object, internal_datasets)
        if not existing_dataset:
            commands.set_dataset(context, internal_manifest, dataset_name, dataset_object)
            continue

        merge_manifest_attributes(existing_dataset, dataset_object, FIELDS_TO_MERGE_FOR_DATASET)
        if (new_name := existing_dataset.name) != existing_dataset_name:
            commands.set_dataset(context, internal_manifest, new_name, existing_dataset)
            commands.get_datasets(context, internal_manifest).pop(existing_dataset_name, None)

        merge_manifest_resources(context, internal_manifest, dataset_name, dataset_object)


def merge_manifest_resources(
    context: Context,
    internal_manifest: Manifest,
    dataset_name: str,
    dataset_object: Dataset,
) -> None:
    external_resources = dataset_object.resources
    internal_resources = commands.get_dataset_resources(context, internal_manifest, dataset_name)
    for resource_name, resource_object in external_resources.items():
        existing_resource, existing_resource_name = find_existing_entity(
            resource_object, internal_resources, ("id", "name", "external")
        )
        if not existing_resource:
            commands.set_resource(context, internal_manifest, dataset_object.name, resource_name, resource_object)
            continue

        merge_manifest_attributes(existing_resource, resource_object, FIELDS_TO_MERGE_FOR_RESOURCE)
        if (new_name := existing_resource.name) != existing_resource_name:
            commands.set_resource(context, internal_manifest, dataset_object.name, new_name, existing_resource)
            commands.get_dataset_resources(context, internal_manifest, dataset_object.name).pop(
                existing_resource_name, None
            )

        merge_manifest_models(context, internal_manifest, dataset_object, resource_name, resource_object)


def merge_manifest_models(
    context: Context,
    internal_manifest: Manifest,
    dataset: Dataset,
    resource_name: str,
    resource_object: Resource,
) -> None:
    external_models = resource_object.models
    internal_models = commands.get_dataset_resources(context, internal_manifest, dataset.name)[resource_name].models
    for model_name, model_object in external_models.items():
        existing_model, existing_model_name = find_existing_entity(
            model_object, internal_models, ("id", "name", "external.name")
        )
        if not existing_model:
            commands.set_model(context, internal_manifest, model_name, model_object)
            continue

        merge_manifest_attributes(existing_model, model_object, FIELDS_TO_MERGE_FOR_MODEL)
        if (new_name := existing_model.name) != existing_model_name:
            commands.set_model(context, internal_manifest, new_name, existing_model)
            commands.get_models(context, internal_manifest).pop(existing_model_name)

        merge_manifest_properties(context, internal_manifest, model_name, model_object)


def merge_manifest_properties(
    context: Context,
    internal_manifest: Manifest,
    model_name: str,
    model_object: Model,
) -> None:
    internal_properties = {
        property_name: _property
        for property_name, _property in commands.get_model_properties(context, internal_manifest, model_name).items()
        if not property_name.startswith(PRIVATE_PROPERTY_PREFIX)
    }
    external_properties = {
        property_name: _property
        for property_name, _property in model_object.properties.items()
        if not property_name.startswith(PRIVATE_PROPERTY_PREFIX)
    }
    for property_name, property_object in external_properties.items():
        existing_property, existing_property_name = find_existing_entity(
            property_object, internal_properties, keys=("id", "name", "external.name")
        )
        if not existing_property:
            commands.set_property(context, internal_manifest, model_name, property_object.name, property_object)
            continue

        merge_manifest_attributes(existing_property, property_object, FIELDS_TO_MERGE_FOR_PROPERTY)
        if (new_name := existing_property.name) != existing_property_name:
            commands.set_property(context, internal_manifest, model_name, new_name, existing_property)
            commands.get_model_properties(context, internal_manifest, model_name).pop(existing_property_name)
