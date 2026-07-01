import collections
import uuid
from typing import Any, Dict, Iterable, Iterator, List, Optional, TypedDict, overload

from starlette.requests import Request
from starlette.responses import Response
from toposort import toposort

from spinta import commands, exceptions
from spinta.auth import authorized
from spinta.backends.constants import BackendFeatures
from spinta.backends.nobackend.components import NoBackend
from spinta.components import Config, Context, Model, Namespace, Node, UrlParams
from spinta.core.enums import Action
from spinta.exceptions import InvalidManifestFile, InvalidName
from spinta.manifests.components import Manifest
from spinta.nodes import load_node
from spinta.types.datatype import Array, Ref
from spinta.utils.naming import is_valid_namespace_name

RESERVED_NAMES = {
    "_schema",
}


class NamespaceData(TypedDict):
    title: str
    description: str


def load_namespace_from_name(
    context: Context,
    manifest: Manifest,
    path: str,
    *,
    # Drop last element from path which is usually a model name.
    drop: bool = True,
) -> Namespace:
    ns: Optional[Namespace] = None
    parts: List[str] = []
    parts_ = [p for p in path.split("/") if p]
    if drop:
        parts_ = parts_[:-1]
    for part in [""] + parts_:
        parts.append(part)
        name = "/".join(parts[1:])
        if not commands.has_namespace(context, manifest, name, loaded=True):
            ns = Namespace()
            data = {
                "type": "ns",
                "name": name,
                "title": "",
                "description": "",
            }
            commands.load(context, ns, data, manifest)
            ns.generated = True
            commands.link(context, ns)
        else:
            ns = commands.get_namespace(context, manifest, name)
            pass

    return ns


def merge_declared_namespace(ns: Namespace, eid: Any, data: Dict[str, Any]) -> Namespace:
    """Apply an explicit ``ns`` declaration onto an already existing namespace.

    A namespace may already exist because it was generated from a model/dataset
    path or because it was declared earlier. Instead of creating a duplicate (or
    erroring out in the generic ``get_node`` check, which was order dependent),
    the declared metadata is merged onto the existing node, preserving its
    collected children (``names``/``models``). If the namespace was already
    explicitly declared, it is flagged as ``redeclared`` so the duplicate is
    reported during the check phase (#1271).
    """
    if not ns.generated:
        ns.redeclared = True
    ns.generated = False
    ns.eid = eid
    ns.type = data["type"]
    ns.title = data.get("title") or ""
    ns.description = data.get("description") or ""
    return ns


def ensure_model_namespace(context: Context, manifest: Manifest, model: Model) -> Namespace:
    """Attach a model to its namespace, generating the namespace if needed.

    Idempotent: safe to call more than once (e.g. from ``build_namespaces`` and
    again from the model ``link`` command used by the lazy internal_sql path).
    """
    if model.ns is not None:
        return model.ns
    ns = load_namespace_from_name(context, manifest, model.name)
    ns.models[model.model_type()] = model
    model.ns = ns
    return ns


def ensure_dataset_namespace(context: Context, manifest: Manifest, dataset) -> Namespace:
    """Attach a dataset to its namespace, generating the namespace if needed.

    Idempotent, see :func:`ensure_model_namespace`.
    """
    if dataset.ns is not None:
        return dataset.ns
    dataset.ns = load_namespace_from_name(context, manifest, dataset.name, drop=False)
    return dataset.ns


def build_namespaces(context: Context, manifest: Manifest) -> None:
    """Generate the namespace tree from datasets and models.

    Namespaces used to be generated during loading (interleaved with node
    loading), which made duplicate handling order dependent. They are now
    collected here, once all nodes are loaded, before nodes are linked
    (see #1271).
    """
    for dataset in commands.get_datasets(context, manifest).values():
        ensure_dataset_namespace(context, manifest, dataset)
    for model in commands.get_models(context, manifest).values():
        ensure_model_namespace(context, manifest, model)


@commands.load.register(Context, Namespace, dict, Manifest)
def load(
    context: Context,
    ns: Namespace,
    data: Dict[str, Any],
    manifest: Manifest,
    *,
    source: Manifest = None,
):
    load_node(context, ns, data)
    ns.path = manifest.path
    ns.manifest = manifest
    ns.access = manifest.access
    ns.backend = None
    ns.names = {}
    ns.models = {}
    commands.set_namespace(context, manifest, ns.name, ns)


@commands.link.register(Context, Namespace)
def link(context: Context, ns: Namespace):
    split = ns.name.split("/")
    if len(split) > 1:
        parent_ns = commands.get_namespace(context, ns.manifest, "/".join(split[:-1]))
        if parent_ns:
            ns.parent = parent_ns
    elif ns.name != "":
        if commands.has_namespace(context, ns.manifest, ""):
            ns.parent = commands.get_namespace(context, ns.manifest, "")
    else:
        ns.parent = ns.manifest

    if isinstance(ns.parent, Namespace):
        ns.parent.names[ns.name] = ns


@commands.check.register(Context, Namespace)
def check(context: Context, ns: Namespace):
    config: Config = context.get("config")

    if ns.redeclared:
        raise InvalidManifestFile(
            ns,
            manifest=ns.manifest.name,
            eid=ns.eid,
            error=f"'ns' with name {ns.name!r} is already defined.",
        )

    if config.check_names:
        name = ns.name
        if name and name not in RESERVED_NAMES:
            if not is_valid_namespace_name(name):
                raise InvalidName(ns, name=name, type="namespace")


@commands.authorize.register(Context, Action, Namespace)
def authorize(context: Context, action: Action, ns: Namespace):
    commands.authorize(context, action, ns, ns.manifest)


@commands.getall.register(Context, Namespace, Request)
async def getall(
    context: Context, ns: Namespace, request: Request, *, action: Action, params: UrlParams, **kwargs
) -> Response:
    config: Config = context.get("config")
    if config.root and ns.is_root():
        ns = commands.get_namespace(context, ns.manifest, config.root)

    commands.authorize(context, action, ns)

    accesslog = context.get("accesslog")
    accesslog.request(
        # XXX: Read operations does not have a transaction, but it
        #      is needed for loging.
        txn=str(uuid.uuid4()),
        ns=ns.name,
        action=action.value,
    )
    return commands.getall(context, ns, request, ns.manifest, action=action, params=params)


@commands.getall.register(Context, Namespace)
def getall(
    context: Context,
    ns: Namespace,
    *,
    action: Optional[Action] = None,
    dataset_: Optional[str] = None,
    resource: Optional[str] = None,
    **kwargs,
):
    return _query_data(context, ns, action, dataset_, resource, **kwargs)


def _query_data(
    context: Context,
    ns: Namespace,
    action: Action,
    dataset_: Optional[str] = None,
    resource: Optional[str] = None,
    **kwargs,
):
    models = commands.traverse_ns_models(
        context,
        ns,
        ns.manifest,
        action,
        dataset_=dataset_,
        resource=resource,
        internal=True,
    )
    for model in models:
        yield from commands.getall(context, model, model.backend, **kwargs)


def check_if_model_has_backend_and_source(model: Model):
    return not isinstance(model.backend, NoBackend) and (model.external and model.external.name)


def _model_matches_params(
    context: Context,
    model: Model,
    action: Action,
    dataset_: Optional[str] = None,
    resource: Optional[str] = None,
    internal: bool = False,
):
    if not internal and model.name.startswith("_"):
        return False

    if not authorized(context, model, action):
        return False

    if dataset_ is None and resource is None:
        return True

    if model.external is None:
        return False

    if dataset_ is not None and (model.external.dataset is None or model.external.dataset.name != dataset_):
        return False

    if resource is not None and (model.external.resource is None or model.external.resource.name != resource):
        return False

    return True


@commands.getone.register(Context, Request, Namespace)
async def getone(
    context: Context,
    request: Request,
    ns: Namespace,
    *,
    action: Action,
    params: UrlParams,
) -> Response:
    raise exceptions.ModelNotFound(model=ns.name)


@overload
@commands.in_namespace.register()  # noqa
def in_namespace(node: Node, ns: Namespace) -> bool:
    return node.name.startswith(ns.name)


@commands.in_namespace.register()  # noqa
def in_namespace(node: Node, parent: Node) -> bool:  # noqa
    while node:
        if node is parent:
            return True
        node = node.parent
    return False


@commands.wipe.register(Context, Namespace, type(None))
def wipe(context: Context, ns: Namespace, backend: type(None)):
    commands.authorize(context, Action.WIPE, ns)
    models = commands.traverse_ns_models(context, ns, ns.manifest, Action.WIPE, internal=True)
    models = sort_models_by_refs(models)
    for model in models:
        if BackendFeatures.WRITE in model.backend.features:
            commands.wipe(context, model, model.backend)


def sort_models_by_refs(models: Iterable[Model]) -> Iterator[Model]:
    models = {model.model_type(): model for model in models}
    graph = collections.defaultdict(set)
    for name, model in models.items():
        if model.base is None:
            graph[""].add(name)
        if model.base:
            base_model = model.base
            while base_model:
                graph[base_model.parent.name].add(name)
                if base_model.parent.base:
                    base_model = base_model.parent.base
                else:
                    base_model = None
        for dtype in iter_model_refs(model):
            ref = dtype.model.model_type()
            if ref in models:
                graph[ref].add(name)
    graph = toposort(graph)
    seen = {""}
    for group in graph:
        for name in sorted(group):
            if name in seen:
                continue
            seen.add(name)
            yield models[name]


def sort_models_by_base(models: Iterable[Model]) -> Iterator[Model]:
    models = {model.model_type(): model for model in models}
    graph = collections.defaultdict(set)
    for name, model in models.items():
        graph[""].add(name)
        for base in iter_model_base(model):
            ref = base.model_type()
            if ref in models:
                graph[ref].add(name)
    graph = toposort(graph)
    seen = {""}
    for group in graph:
        for name in sorted(group):
            if name in seen:
                continue
            seen.add(name)
            yield models[name]


def _topological_sort(model: Model, visited: dict, stack: list, dependencies: dict):
    visited[model.model_type()] = True
    for neighbor in dependencies[model.model_type()]:
        if not visited[neighbor.model_type()]:
            _topological_sort(neighbor, visited, stack, dependencies)
    stack.append(model)


def sort_models_by_ref_and_base(models: List[Model]):
    dependencies = collections.defaultdict(list)
    for model in models:
        if model.base and commands.identifiable(model.base):
            if model.base.parent in models:
                dependencies[model.model_type()].append(model.base.parent)
        for ref in iter_model_refs(model):
            if commands.identifiable(ref.prop):
                if ref.model in models:
                    dependencies[model.model_type()].append(ref.model)

    visited = {model.model_type(): False for model in models}
    stack = []

    for model in models:
        if not visited[model.model_type()]:
            _topological_sort(model, visited, stack, dependencies)

    return stack


def iter_model_base(model: Model) -> Iterator[Model]:
    if model.base:
        yield from iter_model_base(model.base.parent)
        yield model.base.parent


def iter_model_refs(model: Model) -> Iterator[Ref]:
    for prop in model.flatprops.values():
        if isinstance(prop.dtype, Array):
            prop = prop.dtype.items
        if isinstance(prop.dtype, Ref):
            yield prop.dtype
