from typing import Dict
import sqlalchemy as sa
from spinta import commands
from spinta.components import Model, Namespace, Context
from spinta.datasets.components import Dataset
from spinta.manifests.internal_sql.components import InternalSQLManifest
from spinta.manifests.internal_sql.helpers import internal_to_schema, load_internal_manifest_nodes
from spinta.types.namespace import load_namespace_from_name


@commands.has_model.register(Context, InternalSQLManifest, str)
def has_model(context: Context, manifest: InternalSQLManifest, model: str, **kwargs):
    return model in manifest.get_objects()['model']


@commands.get_model.register(Context, InternalSQLManifest, str)
def get_model(context: Context, manifest: InternalSQLManifest, model: str, **kwargs):
    if has_model(context, manifest, model):
        return manifest.get_objects()['model'][model]
    raise Exception("MODEL NOT FOUND")


@commands.get_models.register(Context, InternalSQLManifest)
def get_models(context: Context, manifest: InternalSQLManifest, **kwargs):
    return manifest.get_objects()['model']


@commands.set_model.register(Context, InternalSQLManifest, str, Model)
def set_model(context: Context, manifest: InternalSQLManifest, model_name: str, model: Model, **kwargs):
    manifest.get_objects()['model'][model_name] = model


@commands.set_models.register(Context, InternalSQLManifest, dict)
def set_models(context: Context, manifest: InternalSQLManifest, models: Dict[str, Model], **kwargs):
    manifest.get_objects()['model'] = models


@commands.has_namespace.register(Context, InternalSQLManifest, str)
def has_namespace(context: Context, manifest: InternalSQLManifest, namespace: str, check_only_loaded: bool = False, **kwargs):
    manifest = context.get('request.manifest')
    conn = context.get('transaction.manifest').connection
    if namespace in manifest.get_objects()['ns']:
        return True
    elif not check_only_loaded:
        table = manifest.table
        ns = conn.execute(
            sa.select(table).where(table.c.mpath.startswith(namespace)).limit(1)
        )
        if any(ns):
            return True
    return False


@commands.get_namespace.register(Context, InternalSQLManifest, str)
def get_namespace(context: Context, manifest: InternalSQLManifest, namespace: str, **kwargs):
    manifest = context.get('request.manifest')
    conn = context.get('transaction.manifest').connection
    objects = manifest.get_objects()

    if has_namespace(context, manifest, namespace):
        if namespace in objects['ns']:
            ns = objects['ns'][namespace]
            return ns
        else:
            table = manifest.table
            ns = conn.execute(
                sa.select(table).where(
                    sa.and_(
                        table.c.name == namespace,
                        table.c.dim == 'ns'
                    )
                )
            )
            schemas = internal_to_schema(manifest, ns)
            load_internal_manifest_nodes(context, manifest, schemas)
            if namespace in objects['ns']:
                return objects['ns'][namespace]

            ns = load_namespace_from_name(context, manifest, namespace, drop=False)
            return ns

    raise Exception("NAMESPACE NOT FOUND")


@commands.get_namespaces.register(Context, InternalSQLManifest)
def get_namespaces(context: Context, manifest: InternalSQLManifest, **kwargs):
    return manifest.get_objects()['ns']


@commands.set_namespace.register(Context, InternalSQLManifest, str, Namespace)
def set_namespace(context: Context, manifest: InternalSQLManifest, namespace: str, ns: Namespace, **kwargs):
    manifest = context.get('request.manifest')
    manifest.get_objects()['ns'][namespace] = ns


@commands.has_dataset.register(Context, InternalSQLManifest, str)
def has_dataset(context: Context, manifest: InternalSQLManifest, dataset: str, **kwargs):
    return dataset in manifest.get_objects()['dataset']


@commands.get_dataset.register(Context, InternalSQLManifest, str)
def get_dataset(context: Context, manifest: InternalSQLManifest, dataset: str, **kwargs):
    if has_dataset(context, manifest, dataset):
        return manifest.get_objects()['dataset'][dataset]
    raise Exception("DATASET NOT FOUND")


@commands.get_datasets.register(Context, InternalSQLManifest)
def get_datasets(context: Context, manifest: InternalSQLManifest, **kwargs):
    return manifest.get_objects()['dataset']


@commands.set_dataset.register(Context, InternalSQLManifest, str, Dataset)
def set_dataset(context: Context, manifest: InternalSQLManifest, dataset_name: str, dataset: Dataset, **kwargs):
    manifest.get_objects()['dataset'][dataset_name] = dataset

