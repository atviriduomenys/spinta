from copy import copy
from typing import Any, List, Union
from typing import Callable
from typing import Dict
from typing import Hashable
from typing import Iterator
from typing import Tuple
from typing import TypeVar

from spinta import commands
from spinta.cli.helpers.auth import require_auth
from spinta.cli.helpers.store import load_manifest
from spinta.components import Context, Property, Node
from spinta.core.enums import Mode
from spinta.components import Model
from spinta.core.config import RawConfig, Path
from spinta.core.config import ResourceTuple
from spinta.core.config import parse_resource_args
from spinta.core.context import configure_context, create_context
from spinta.datasets.components import Dataset, Resource, ExternalBackend
from spinta.datasets.inspect.components import PriorityKey
from spinta.exceptions import InvalidResourceSource
from spinta.manifests.components import Manifest
from spinta.manifests.components import ManifestPath
from spinta.manifests.helpers import init_manifest
from spinta.utils.naming import Deduplicator
from spinta.utils.schema import NA


def create_manifest_from_inspect(
    context: Context = None,
    manifest: Union[str, ManifestPath] = None,
    resources: Tuple = None,
    backend: str = None,
    formula: str = None,
    dataset: str = None,
    auth: str = None,
    priority: str = "manifest",
    only_url: bool = False,
):
    # Reset Context, to not have any outside influence from given
    if context is None:
        rc = RawConfig()
        rc.read([Path("spinta", "spinta.config:CONFIG")])
        context = create_context(name="inspect", rc=rc)

    has_manifest_priority = priority == "manifest"
    if resources:
        resources = parse_resource_args(*resources, formula)

    context = configure_context(context, [manifest] if manifest else None, mode=Mode.external, backend=backend)
    with context:
        require_auth(context, auth)
        store = load_manifest(context, ensure_config_dir=True, full_load=True)
        old = store.manifest
        manifest = Manifest()
        init_manifest(context, manifest, "inspect")
        commands.merge(context, manifest, manifest, old, has_manifest_priority)

        if not resources:
            resources = []
            for ds in commands.get_datasets(context, old).values():
                for resource in ds.resources.values():
                    external = resource.external
                    if external == "" and resource.backend:
                        external = resource.backend.config["dsn"]
                    if not any(res.external == external for res in resources):
                        if only_url and not ("http://" in external or "https://" in external):
                            raise InvalidResourceSource(source=external)
                        resources.append(ResourceTuple(type=resource.type, external=external, prepare=resource.prepare))

        if resources:
            for resource in resources:
                _merge(context, manifest, manifest, resource, has_manifest_priority, dataset)

        # Sort models for render
        sorted_models = {}
        for key, model in commands.get_models(context, manifest).items():
            if key not in sorted_models.keys():
                if model.external and model.external.resource:
                    resource = model.external.resource
                    for resource_key, resource_model in resource.models.items():
                        if resource_key not in sorted_models.keys():
                            sorted_models[resource_key] = resource_model
                else:
                    sorted_models[key] = model
        commands.set_models(context, manifest, sorted_models)
    return context, manifest


def _merge(
    context: Context,
    manifest: Manifest,
    old: Manifest,
    resource: ResourceTuple,
    has_manifest_priority: bool,
    dataset: str = None,
):
    manifest_ = commands.backend_to_manifest_type(context, resource.type)
    path = ManifestPath(type=manifest_.type, path=resource.external, prepare=resource.prepare)
    context = configure_context(context, [path], mode=Mode.external, dataset=dataset)
    store = load_manifest(context, full_load=True)
    new = store.manifest
    commands.merge(context, manifest, old, new, has_manifest_priority)


def _filter_models_for_dataset(context: Context, manifest: Manifest, dataset: Dataset) -> List[Model]:
    models = []
    for model in commands.get_models(context, manifest).values():
        if model.external:
            if model.external.dataset is dataset:
                models.append(model)
    return models


def _merge_model_properties(context: Context, manifest: Manifest, old: Model, new: Model, has_manifest_priority: bool):
    properties = zipitems(
        old.properties.values(),
        new.properties.values(),
        _property_key,
    )
    deduplicator = Deduplicator("_{}")
    for prop in properties:
        for o, n in prop:
            if n:
                n = copy(n)
                n.model = new
            if o:
                deduplicator(o.basename)
                o.model = old
            if n and not o:
                name = deduplicator(n.basename)
                n.model = old
                n.name = name
                n.place = name
            commands.merge(context, manifest, o, n, has_manifest_priority)


def _merge_prefixes(context: Context, manifest: Manifest, old: Dataset, new: Dataset):
    prefixes = zipitems(
        old.prefixes.values(),
        new.prefixes.values(),
        _name_key,
    )
    for prefix in prefixes:
        for o, n in prefix:
            commands.merge(context, manifest, o, n)


def _merge_resources(context: Context, manifest: Manifest, old: Dataset, new: Dataset):
    old_resources = [] if old == NA else old.resources.values()
    new_resources = [] if new == NA else new.resources.values()
    resources = zipitems(
        old_resources,
        new_resources,
        _resource_key,
    )
    deduplicator = Deduplicator("{}")
    for res in resources:
        for o, n in res:
            if o:
                deduplicator(o.name)
            elif n:
                n.name = deduplicator(n.name)
            commands.merge(context, manifest, o, n)


TItem = TypeVar("TItem")


def zipitems(
    a: TItem,
    b: TItem,
    key: Callable[[TItem], Hashable],
) -> Iterator[List[Tuple[TItem, TItem]]]:
    res: Dict[
        Hashable,  # key
        List[
            List[
                Any,  # a
                Any,  # b
            ]
        ],
    ] = {}

    # Map first values
    for value in a:
        value_key = key(value)
        new_value = [value, NA]

        if isinstance(value_key, Tuple):
            for existing_key in res.keys():
                if set(value_key) == set(existing_key):
                    value_key = existing_key
                    break
        elif isinstance(value_key, PriorityKey):
            for existing_key in res.keys():
                if value_key == existing_key:
                    value_key = existing_key
                    break

        res.setdefault(value_key, []).append(new_value)

    # Map second value
    for value in b:
        value_key = key(value)
        new_value = [NA, value]

        mapped_keys = []
        if isinstance(value_key, Tuple):
            value_key_set = set(value_key)
            mapped_keys = [existing_key for existing_key in res if value_key_set.issubset(set(existing_key))]
        elif isinstance(value_key, PriorityKey):
            # Only get the first match
            for existing_key in res:
                if existing_key == value_key:
                    mapped_keys = [existing_key]
                    break

        if not mapped_keys:
            mapped_keys = [value_key]

        for mapped_key in mapped_keys:
            if mapped_key not in res or not res[mapped_key]:
                res[mapped_key] = [new_value]
                continue

            extension_list = []
            for existing_value in res[mapped_key]:
                if existing_value[1] is NA:
                    existing_value[1] = value
                else:
                    extension_list.append([existing_value[0], value])
            res[mapped_key] += extension_list

    yield from res.values()


def coalesce(*args: Any) -> Any:
    for arg in args:
        if arg:
            return arg
    return arg


def _name_key(node: Node) -> str:
    return node.name


def _dataset_key(dataset: Dataset) -> PriorityKey:
    key = PriorityKey()
    if dataset.id:
        key.id = dataset.id
    if dataset.given.name:
        key.name = dataset.given.name
    key.source = _dataset_resource_source_key(dataset)
    return key


def _dataset_resource_source_key(dataset: Dataset) -> Tuple:
    keys = []
    for resource in dataset.resources.values():
        keys.append(_resource_source_key(resource))
    return tuple(keys)


def _property_key(prop: Property) -> PriorityKey:
    key = PriorityKey()
    if prop.id:
        key.id = prop.id
    if prop.given.name:
        key.name = prop.given.name
    key.source = _property_source_key(prop)
    return key


def _property_source_key(prop: Property) -> str:
    if prop.external and prop.external.name:
        return prop.external.name
    else:
        return prop.name


def _resource_key(resource: Resource) -> PriorityKey:
    key = PriorityKey()
    if resource.id:
        key.id = resource.id
    if resource.given.name:
        key.name = resource.given.name
    key.source = _resource_source_key(resource)
    return key


def _resource_source_key(resource: Resource) -> str:
    result = resource.name
    manifest = resource.dataset.manifest
    if resource.external:
        result = resource.external
    else:
        if manifest.backends:
            if resource.backend.name in manifest.backends:
                result = manifest.backends[resource.backend.name].config["dsn"]
        elif manifest.store.backends:
            if resource.backend.name in manifest.store.backends:
                result = manifest.store.backends[resource.backend.name].config["dsn"]
    if "@" in result:
        result = result.split("@")[1]
    return result


def _model_key(model: Model) -> PriorityKey:
    key = PriorityKey()
    if model.id:
        key.id = model.id
    if model.given.name:
        key.name = model.given.name
    key.source = _model_source_key(model)
    return key


def _model_source_key(model: Model) -> str:
    if model.external and model.external.name:
        return f"{_resource_source_key(model.external.resource)}/{model.external.name}"
    else:
        return model.name


def _backend_dsn(backend: ExternalBackend) -> str:
    dsn = backend.config["dsn"]
    if "@" in dsn:
        dsn = dsn.split("@")[1]
    return dsn
