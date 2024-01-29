import os
import tempfile
from pathlib import Path
from uuid import UUID

from spinta import commands
from spinta.cli.helpers.store import prepare_manifest
from spinta.cli.migrate import MigrateMeta, MigrateRename
from spinta.components import Context, UrlParams, Store, Model, Config, Node, ExtraMetaData, Property
from starlette.requests import Request

from spinta.core.context import configure_context, create_context
from spinta.datasets.commands.check import check_dataset_name
from spinta.datasets.inspect.helpers import zipitems
from spinta.exceptions import NotSupportedManifestType, InvalidSchemaUrlPath, InvalidName, UnknownContentType, \
    FileSizeTooLarge, DatasetNameMissmatch, DatasetSchemaRequiresIds, \
    ModifySchemaRequiresFile, ModifyOneDatasetSchema
from spinta.manifests.components import ManifestPath, Manifest
from spinta.manifests.internal_sql.helpers import datasets_to_sql
from spinta.manifests.tabular.helpers import datasets_to_tabular
from spinta.utils.schema import NA
from spinta.utils.types import is_str_uuid


def _clean_up_file(file):
    os.unlink(file.name)


def _setup_context(main_context: Context, manifest_path) -> Context:
    context = create_context("Target Manifest", rc=main_context.get('rc'))
    context = configure_context(context, [manifest_path])
    return context


def validate_manifest(context: Context, manifest: Manifest, dataset_name: str):
    datasets = commands.get_datasets(context, manifest)

    if len(datasets) != 1:
        raise ModifyOneDatasetSchema(manifest, given_amount=len(datasets))

    for dataset in datasets.values():
        if dataset.name != dataset_name:
            raise DatasetNameMissmatch(dataset, expected_dataset=dataset_name, given_dataset=dataset.name)

    rows = datasets_to_tabular(context, manifest)
    for row in rows:
        if row['id'] is not None and not is_str_uuid(row['id']):
            raise DatasetSchemaRequiresIds(manifest)


def create_migrate_rename_mapping(old_context: Context, new_context: Context, old_manifest: Manifest, new_manifest: Manifest, dataset_name: str):
    if not commands.has_dataset(old_context, old_manifest, dataset_name):
        return {}

    old_models = commands.get_dataset_models(old_context, old_manifest, dataset_name)
    new_models = commands.get_dataset_models(new_context, new_manifest, dataset_name)
    models = zipitems(
        old_models.values(),
        new_models.values(),
        _id_model_key
    )
    mapped_data = {}
    for model in models:
        for old, new in model:
            if old is not NA and new is not NA:
                possible_rename = {}
                if new.name != old.name:
                    possible_rename[''] = new.name

                filtered_old_properties = [prop for prop in old.properties.values() if not prop.name.startswith("_")]
                filtered_new_properties = [prop for prop in new.properties.values() if not prop.name.startswith("_")]
                properties = zipitems(
                    filtered_old_properties,
                    filtered_new_properties,
                    _id_property_key
                )

                for prop in properties:
                    for old_prop, new_prop in prop:
                        if old_prop is not NA and new_prop is not NA:
                            if old_prop.name != new_prop.name:
                                possible_rename[old_prop.name] = new_prop.name

                if possible_rename:
                    mapped_data[old.name] = possible_rename
    return mapped_data


def _id_model_key(model: Model):
    id_ = model.id
    return _id_key(id_)


def _id_property_key(prop: Property):
    id_ = prop.id
    return _id_key(id_)


def _id_key(id_):
    if isinstance(id_, UUID):
        id_ = str(id_)
    return id_


def _parse_and_validate_dataset_name(context: Context, manifest: Manifest, params: UrlParams):
    ns = params.model
    if isinstance(ns, Model):
        raise InvalidSchemaUrlPath(ns)

    if ns is not None:
        dataset_name = ns.name
    else:
        dataset_name = '/'.join(params.path_parts)

    if not commands.has_dataset(context, manifest, dataset_name):
        if commands.has_namespace(context, manifest, dataset_name):
            ns = commands.get_namespace(context, manifest, dataset_name)
            raise InvalidSchemaUrlPath(ns)

    if not check_dataset_name(dataset_name):
        raise InvalidName(name=dataset_name, type='dataset')

    return dataset_name


def _create_and_validate_tmp_file(context: Context, manifest: Manifest, request: Request):
    headers = request.headers
    if 'content-type' not in headers:
        raise ModifySchemaRequiresFile()
    if 'content-type' in headers and headers['content-type'] != 'text/csv':
        raise UnknownContentType(content_type=headers['content-type'], supported_content_types=['text/csv'])

    config: Config = context.get('config')
    max_size = config.max_api_file_size
    data_stream = request.stream()
    tmp = tempfile.NamedTemporaryFile(delete=False)

    file_is_valid = True
    async for data_block in data_stream:
        tmp.write(data_block)
        if tmp.tell() > max_size * 1e+6:
            file_is_valid = False
            break
    tmp.close()

    if not file_is_valid:
        _clean_up_file(tmp)
        raise FileSizeTooLarge(manifest, allowed_amount=max_size, measure='MB')

    return tmp


async def schema_api(context: Context, request: Request, params: UrlParams):
    store: Store = context.get('store')
    manifest = store.manifest

    if not manifest.dynamic:
        raise NotSupportedManifestType(manifest, manifest_name=manifest.name, supported_type="dynamic", given_type="static")

    dataset_name = _parse_and_validate_dataset_name(context, manifest, params)
    tmp_file = _create_and_validate_tmp_file(context, manifest, request)

    try:
        manifest_path = ManifestPath(type='csv', path=tmp_file.name)
        target_context = _setup_context(context, manifest_path)
        store = prepare_manifest(target_context, ensure_config_dir=True, full_load=True)
        target_manifest = store.manifest
        validate_manifest(context, target_manifest, dataset_name)

        rename_data = create_migrate_rename_mapping(context, target_context, manifest, target_manifest, dataset_name)
        migrate_meta = MigrateMeta(
            plan=False,
            autocommit=False,
            rename=MigrateRename(
                rename_src=rename_data
            ),
            datasets=[dataset_name]
        )
        commands.migrate(context, target_manifest, migrate_meta)
        commands.update_manifest_dataset_schema(context, manifest, target_manifest)

    except Exception as e:
        # Ensure that tmp file is deleted if there is any exception
        _clean_up_file(tmp_file)
        raise e

    _clean_up_file(tmp_file)
