import collections
import re
import uuid
from typing import Any
from typing import Dict
from typing import Iterable
from typing import Iterator
from typing import List
from typing import Optional
from typing import TypedDict
from typing import overload

from starlette.requests import Request
from starlette.responses import Response
from toposort import toposort

from spinta import commands
from spinta import exceptions
from spinta.auth import authorized
from spinta.backends.constants import BackendFeatures
from spinta.backends.nobackend.components import NoBackend
from spinta.components import Action
from spinta.components import Config
from spinta.components import Context
from spinta.components import Model
from spinta.components import Namespace
from spinta.components import Node
from spinta.components import UrlParams
from spinta.exceptions import InvalidName
from spinta.manifests.components import Manifest
from spinta.nodes import load_node
from spinta.types.datatype import Ref

namespace_is_lowercase = re.compile(r'^([a-z][a-z0-9]*)+(/[a-z][a-z0-9]*)+|([a-z][a-z0-9]*)$')

RESERVED_NAMES = {
    '_schema',
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
    parts_ = [p for p in path.split('/') if p]
    if drop:
        parts_ = parts_[:-1]
    for part in [''] + parts_:
        parts.append(part)
        name = '/'.join(parts[1:])
        if not commands.has_namespace(context, manifest, name, loaded=True):
            ns = Namespace()
            data = {
                'type': 'ns',
                'name': name,
                'title': '',
                'description': '',
            }
            commands.load(context, ns, data, manifest)
            ns.generated = True
            commands.link(context, ns)
        else:
            ns = commands.get_namespace(context, manifest, name)
            pass

    return ns


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
    split = ns.name.split('/')
    if len(split) > 1:
        parent_ns = commands.get_namespace(context, ns.manifest, '/'.join(split[:-1]))
        if parent_ns:
            ns.parent = parent_ns
    elif ns.name != '':
        if commands.has_namespace(context, ns.manifest, ''):
            ns.parent = commands.get_namespace(context, ns.manifest, '')
    else:
        ns.parent = ns.manifest

    if isinstance(ns.parent, Namespace):
        ns.parent.names[ns.name] = ns


@commands.check.register(Context, Namespace)
def check(context: Context, ns: Namespace):
    config: Config = context.get('config')

    if config.check_names:
        name = ns.name
        if name and name not in RESERVED_NAMES:
            if namespace_is_lowercase.match(name) is None:
                raise InvalidName(ns, name=name, type='namespace')


@commands.authorize.register(Context, Action, Namespace)
def authorize(context: Context, action: Action, ns: Namespace):
    commands.authorize(context, action, ns, ns.manifest)


@commands.getall.register(Context, Namespace, Request)
async def getall(
    context: Context,
    ns: Namespace,
    request: Request,
    *,
    action: Action,
    params: UrlParams,
    **kwargs
) -> Response:
    config: Config = context.get('config')
    if config.root and ns.is_root():
        ns = commands.get_namespace(context, ns.manifest, config.root)

    commands.authorize(context, action, ns)

    accesslog = context.get('accesslog')
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
    **kwargs
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

    if not internal and model.name.startswith('_'):
        return False

    if not authorized(context, model, action):
        return False

    if (
        dataset_ is None and
        resource is None
    ):
        return True

    if model.external is None:
        return False

    if (
        dataset_ is not None and (
            model.external.dataset is None or
            model.external.dataset.name != dataset_
        )
    ):
        return False

    if (
        resource is not None and (
            model.external.resource is None or
            model.external.resource.name != resource
        )
    ):
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
            graph[''].add(name)
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
    seen = {''}
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
        graph[''].add(name)
        for base in iter_model_base(model):
            ref = base.model_type()
            if ref in models:
                graph[ref].add(name)
    graph = toposort(graph)
    seen = {''}
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
    for prop in model.properties.values():
        if prop.dtype.name == 'ref':
            yield prop.dtype
