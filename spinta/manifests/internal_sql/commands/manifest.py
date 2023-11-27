from typing import Dict

from spinta import commands
from spinta.components import Model, Namespace, Context
from spinta.datasets.components import Dataset
from spinta.manifests.internal_sql.components import InternalSQLManifest


@commands.has_model.register(Context, InternalSQLManifest, str)
def has_model(context: Context, manifest: InternalSQLManifest, model: str):
    return model in manifest.get_objects()['model']


@commands.get_model.register(Context, InternalSQLManifest, str)
def get_model(context: Context, manifest: InternalSQLManifest, model: str):
    if has_model(context, manifest, model):
        return manifest.get_objects()['model'][model]
    raise Exception("MODEL NOT FOUND")


@commands.get_models.register(Context, InternalSQLManifest)
def get_models(context: Context, manifest: InternalSQLManifest):
    return manifest.get_objects()['model']


@commands.set_model.register(Context, InternalSQLManifest, str, Model)
def set_model(context: Context, manifest: InternalSQLManifest, model_name: str, model: Model):
    manifest.get_objects()['model'][model_name] = model


@commands.set_models.register(Context, InternalSQLManifest, dict)
def set_models(context: Context, manifest: InternalSQLManifest, models: Dict[str, Model]):
    manifest.get_objects()['model'] = models


@commands.has_namespace.register(Context, InternalSQLManifest, str)
def has_namespace(context: Context, manifest: InternalSQLManifest, namespace: str):
    return namespace in manifest.get_objects()['ns']


@commands.get_namespace.register(Context, InternalSQLManifest, str)
def get_namespace(context: Context, manifest: InternalSQLManifest, namespace: str):
    if has_namespace(context, manifest, namespace):
        return manifest.get_objects()['ns'][namespace]
    raise Exception("NAMESPACE NOT FOUND")


@commands.get_namespaces.register(Context, InternalSQLManifest)
def get_namespaces(context: Context, manifest: InternalSQLManifest):
    return manifest.get_objects()['ns']


@commands.set_namespace.register(Context, InternalSQLManifest, str, Namespace)
def set_namespace(context: Context, manifest: InternalSQLManifest, namespace: str, ns: Namespace):
    manifest.get_objects()['ns'][namespace] = ns


@commands.has_dataset.register(Context, InternalSQLManifest, str)
def has_dataset(context: Context, manifest: InternalSQLManifest, dataset: str):
    return dataset in manifest.get_objects()['dataset']


@commands.get_dataset.register(Context, InternalSQLManifest, str)
def get_dataset(context: Context, manifest: InternalSQLManifest, dataset: str):
    if has_dataset(context, manifest, dataset):
        return manifest.get_objects()['dataset'][dataset]
    raise Exception("DATASET NOT FOUND")


@commands.get_datasets.register(Context, InternalSQLManifest)
def get_datasets(context: Context, manifest: InternalSQLManifest):
    return manifest.get_objects()['dataset']


@commands.set_dataset.register(Context, InternalSQLManifest, str, Dataset)
def set_dataset(context: Context, manifest: InternalSQLManifest, dataset_name: str, dataset: Dataset):
    manifest.get_objects()['dataset'][dataset_name] = dataset

