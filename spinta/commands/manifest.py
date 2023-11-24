from typing import TypedDict, Callable, Dict

from spinta import commands
from spinta.components import Namespace, Model, Node
from spinta.datasets.components import Dataset
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


@commands.has_node_type.register(Manifest, str)
def has_object_type(manifest: Manifest, obj_type: str):
    return obj_type in manifest.get_objects()


@commands.has_node.register(Manifest, str, str)
def has_object(manifest: Manifest, obj_type: str, obj: str):
    if obj_type in NODE_FUNCTION_MAPPER:
        return NODE_FUNCTION_MAPPER[obj_type]['has'](manifest, obj)
    raise Exception("NODE NOT DEFINED")


@commands.get_node.register(Manifest, str, str)
def get_node(manifest: Manifest, obj_type: str, obj: str):
    if obj_type in NODE_FUNCTION_MAPPER:
        return NODE_FUNCTION_MAPPER[obj_type]['get'](manifest, obj)
    raise Exception("NODE NOT DEFINED")


@commands.get_nodes.register(Manifest, str)
def get_nodes(manifest: Manifest, obj_type: str):
    if obj_type in NODE_FUNCTION_MAPPER:
        return NODE_FUNCTION_MAPPER[obj_type]['get_all'](manifest)
    raise Exception("NODE NOT DEFINED")


@commands.set_node.register(Manifest, str, str, Node)
def set_node(manifest: Manifest, obj_type: str, obj_name, obj: Node):
    if obj_type in NODE_FUNCTION_MAPPER:
        return NODE_FUNCTION_MAPPER[obj_type]['set'](manifest, obj_name, obj)
    raise Exception("NODE NOT DEFINED")


@commands.has_model.register(Manifest, str)
def has_model(manifest: Manifest, model: str):
    return model in manifest.get_objects()['model']


@commands.get_model.register(Manifest, str)
def get_model(manifest: Manifest, model: str):
    if has_model(manifest, model):
        return manifest.get_objects()['model'][model]
    raise Exception("MODEL NOT FOUND")


@commands.get_models.register(Manifest)
def get_models(manifest: Manifest):
    return manifest.get_objects()['model']


@commands.set_model.register(Manifest, str, Model)
def set_model(manifest: Manifest, model_name: str, model: Model):
    manifest.get_objects()['model'][model_name] = model


@commands.set_models.register(Manifest, dict)
def set_models(manifest: Manifest, models: Dict[str, Model]):
    manifest.get_objects()['model'] = models


@commands.has_namespace.register(Manifest, str)
def has_namespace(manifest: Manifest, namespace: str):
    return namespace in manifest.get_objects()['ns']


@commands.get_namespace.register(Manifest, str)
def get_namespace(manifest: Manifest, namespace: str):
    if has_namespace(manifest, namespace):
        return manifest.get_objects()['ns'][namespace]
    raise Exception("NAMESPACE NOT FOUND")


@commands.get_namespaces.register(Manifest)
def get_namespaces(manifest: Manifest):
    return manifest.get_objects()['ns']


@commands.set_namespace.register(Manifest, str, Namespace)
def set_namespace(manifest: Manifest, namespace: str, ns: Namespace):
    manifest.get_objects()['ns'][namespace] = ns


@commands.has_dataset.register(Manifest, str)
def has_dataset(manifest: Manifest, dataset: str):
    return dataset in manifest.get_objects()['dataset']


@commands.get_dataset.register(Manifest, str)
def get_dataset(manifest: Manifest, dataset: str):
    if has_dataset(manifest, dataset):
        return manifest.get_objects()['dataset'][dataset]
    raise Exception("DATASET NOT FOUND")


@commands.get_datasets.register(Manifest)
def get_datasets(manifest: Manifest):
    return manifest.get_objects()['dataset']


@commands.set_dataset.register(Manifest, str, Dataset)
def set_dataset(manifest: Manifest, dataset_name: str, dataset: Dataset):
    manifest.get_objects()['dataset'][dataset_name] = dataset

