from typing import TypedDict, Callable, Dict

from spinta import commands
from spinta.components import Namespace, Model, Node, Context
from spinta.datasets.components import Dataset
from spinta.exceptions import DatasetNotFound, NamespaceNotFound, ModelNotFound, ManifestObjectNotDefined, \
    NotImplementedFeature
from spinta.manifests.components import Manifest


class _FunctionTypes(TypedDict):
    has: Callable
    get: Callable
    set: Callable
    get_all: Callable


NODE_FUNCTION_MAPPER = {
    'model': _FunctionTypes(
        has=commands.has_model,
        get=commands.get_model,
        set=commands.set_model,
        get_all=commands.get_models
    ),
    'ns': _FunctionTypes(
        has=commands.has_namespace,
        get=commands.get_namespace,
        set=commands.set_namespace,
        get_all=commands.get_namespaces
    ),
    'dataset': _FunctionTypes(
        has=commands.has_dataset,
        get=commands.get_dataset,
        set=commands.set_dataset,
        get_all=commands.get_datasets
    )
}


@commands.has_node_type.register(Context, Manifest, str)
def has_object_type(context: Context, manifest: Manifest, obj_type: str, **kwargs):
    return obj_type in manifest.get_objects()


@commands.has_node.register(Context, Manifest, str, str)
def has_object(context: Context, manifest: Manifest, obj_type: str, obj: str, **kwargs):
    if obj_type in NODE_FUNCTION_MAPPER:
        return NODE_FUNCTION_MAPPER[obj_type]['has'](context, manifest, obj, **kwargs)
    raise ManifestObjectNotDefined(obj=obj_type)


@commands.get_node.register(Context, Manifest, str, str)
def get_node(context: Context, manifest: Manifest, obj_type: str, obj: str, **kwargs):
    if obj_type in NODE_FUNCTION_MAPPER:
        return NODE_FUNCTION_MAPPER[obj_type]['get'](context, manifest, obj, **kwargs)
    raise ManifestObjectNotDefined(obj=obj_type)


@commands.get_nodes.register(Context, Manifest, str)
def get_nodes(context: Context, manifest: Manifest, obj_type: str, **kwargs):
    if obj_type in NODE_FUNCTION_MAPPER:
        return NODE_FUNCTION_MAPPER[obj_type]['get_all'](context, manifest, **kwargs)
    raise ManifestObjectNotDefined(obj=obj_type)


@commands.set_node.register(Context, Manifest, str, str, Node)
def set_node(context: Context, manifest: Manifest, obj_type: str, obj_name, obj: Node, **kwargs):
    if obj_type in NODE_FUNCTION_MAPPER:
        return NODE_FUNCTION_MAPPER[obj_type]['set'](context, manifest, obj_name, obj, **kwargs)
    raise ManifestObjectNotDefined(obj=obj_type)


@commands.has_model.register(Context, Manifest, str)
def has_model(context: Context, manifest: Manifest, model: str, **kwargs):
    return model in manifest.get_objects()['model']


@commands.get_model.register(Context, Manifest, str)
def get_model(context: Context, manifest: Manifest, model: str, **kwargs):
    if has_model(context, manifest, model):
        return manifest.get_objects()['model'][model]
    raise ModelNotFound(model=model)


@commands.get_models.register(Context, Manifest)
def get_models(context: Context, manifest: Manifest, **kwargs):
    return manifest.get_objects()['model']


@commands.set_model.register(Context, Manifest, str, Model)
def set_model(context: Context, manifest: Manifest, model_name: str, model: Model, **kwargs):
    manifest.get_objects()['model'][model_name] = model


@commands.set_models.register(Context, Manifest, dict)
def set_models(context: Context, manifest: Manifest, models: Dict[str, Model], **kwargs):
    manifest.get_objects()['model'] = models


@commands.has_namespace.register(Context, Manifest, str)
def has_namespace(context: Context, manifest: Manifest, namespace: str, **kwargs):
    return namespace in manifest.get_objects()['ns']


@commands.get_namespace.register(Context, Manifest, str)
def get_namespace(context: Context, manifest: Manifest, namespace: str, **kwargs):
    if has_namespace(context, manifest, namespace):
        return manifest.get_objects()['ns'][namespace]
    raise NamespaceNotFound(namespace=namespace)


@commands.get_namespaces.register(Context, Manifest)
def get_namespaces(context: Context, manifest: Manifest, **kwargs):
    return manifest.get_objects()['ns']


@commands.set_namespace.register(Context, Manifest, str, Namespace)
def set_namespace(context: Context, manifest: Manifest, namespace: str, ns: Namespace, **kwargs):
    manifest.get_objects()['ns'][namespace] = ns


@commands.has_dataset.register(Context, Manifest, str)
def has_dataset(context: Context, manifest: Manifest, dataset: str, **kwargs):
    return dataset in manifest.get_objects()['dataset']


@commands.get_dataset.register(Context, Manifest, str)
def get_dataset(context: Context, manifest: Manifest, dataset: str, **kwargs):
    if has_dataset(context, manifest, dataset):
        return manifest.get_objects()['dataset'][dataset]
    raise DatasetNotFound(dataset=dataset)


@commands.get_datasets.register(Context, Manifest)
def get_datasets(context: Context, manifest: Manifest, **kwargs):
    return manifest.get_objects()['dataset']


@commands.set_dataset.register(Context, Manifest, str, Dataset)
def set_dataset(context: Context, manifest: Manifest, dataset_name: str, dataset: Dataset, **kwargs):
    manifest.get_objects()['dataset'][dataset_name] = dataset


@commands.get_dataset_models.register(Context, Manifest, str)
def get_dataset_models(context: Context, manifest: Manifest, dataset_name: str, **kwargs):
    dataset = commands.get_dataset(context, manifest, dataset_name)
    models = commands.get_models(context, manifest)
    filtered_list = {}
    for key, model in models.items():
        if model.external and model.external.dataset == dataset:
            filtered_list[key] = model
    return filtered_list


@commands.update_manifest_dataset_schema.register(Context, Manifest, Manifest)
def update_manifest_dataset_schema(context: Context, manifest: Manifest, target_manifest: Manifest, **kwargs):
    raise NotImplementedFeature(manifest, "Ability to modify non-dynamic manifest's schema")
