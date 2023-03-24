import uuid
import collections
from typing import NamedTuple
from typing import Union
from typing import overload

import itertools
from typing import Any
from typing import Dict
from typing import Iterable
from typing import Iterator
from typing import List
from typing import Optional
from typing import TypedDict

from starlette.requests import Request
from starlette.responses import Response
from toposort import toposort

from spinta import commands
from spinta import exceptions
from spinta.auth import authorized
from spinta.backends.helpers import get_select_prop_names
from spinta.backends.helpers import get_select_tree
from spinta.backends.components import BackendFeatures
from spinta.compat import urlparams_to_expr
from spinta.components import Action
from spinta.components import Config
from spinta.components import Context
from spinta.components import Model
from spinta.components import Namespace
from spinta.components import Node
from spinta.components import UrlParams
from spinta.manifests.components import Manifest
from spinta.nodes import load_node
from spinta.renderer import render
from spinta.types.datatype import Ref
from spinta.accesslog import log_response


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
    parent: Optional[Namespace] = None
    parts: List[str] = []
    parts_ = path.split('/')
    if drop:
        parts_ = parts_[:-1]
    for part in [''] + parts_:
        parts.append(part)
        name = '/'.join(parts[1:])
        if name not in manifest.namespaces:
            ns = Namespace()
            data = {
                'type': 'ns',
                'name': name,
                'title': '',
                'description': '',
            }
            commands.load(context, ns, data, manifest)
            ns.generated = True
        else:
            ns = manifest.namespaces[name]
            pass

        ns.parent = parent or manifest

        if parent and part and part not in parent.names:
            parent.names[part] = ns

        parent = ns

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
    manifest.namespaces[ns.name] = ns


@commands.link.register(Context, Namespace)
def link(context: Context, ns: Namespace):
    pass


@commands.check.register(Context, Namespace)
def check(context: Context, ns: Namespace):
    pass


@commands.authorize.register(Context, Action, Namespace)
def authorize(context: Context, action: Action, ns: Namespace):
    authorized(context, ns, action, throw=True)


@commands.getall.register(Context, Namespace, Request)
async def getall(
    context: Context,
    ns: Namespace,
    request: Request,
    *,
    action: Action,
    params: UrlParams,
) -> Response:
    config: Config = context.get('config')
    if config.root and ns.is_root():
        ns = ns.manifest.namespaces[config.root]

    commands.authorize(context, action, ns)

    accesslog = context.get('accesslog')
    accesslog.request(
        # XXX: Read operations does not have a transaction, but it
        #      is needed for loging.
        txn=str(uuid.uuid4()),
        ns=ns.name,
        action=action.value,
    )

    if params.all and params.ns:

        for model in traverse_ns_models(context, ns, action, internal=True):
            commands.authorize(context, action, model)
        return _get_ns_content(
            context,
            request,
            ns,
            params,
            action,
            recursive=True,
        )
    elif params.all:
        accesslog = context.get('accesslog')

        prepare_data_for_response_kwargs = {}
        for model in traverse_ns_models(context, ns, action, internal=True):
            commands.authorize(context, action, model)
            select_tree = get_select_tree(context, action, params.select)
            prop_names = get_select_prop_names(
                context,
                model,
                model.properties,
                action,
                select_tree,
            )
            prepare_data_for_response_kwargs[model.model_type()] = {
                'select': select_tree,
                'prop_names': prop_names,
            }
        expr = urlparams_to_expr(params)
        rows = getall(context, ns, action=action, query=expr)
        rows = (
            commands.prepare_data_for_response(
                context,
                ns.manifest.models[row['_type']],
                params.fmt,
                row,
                action=action,
                **prepare_data_for_response_kwargs[row['_type']],
            )
            for row in rows
        )
        rows = log_response(context, rows)
        return render(context, request, ns, params, rows, action=action)
    else:
        return _get_ns_content(context, request, ns, params, action)


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
    models = traverse_ns_models(
        context,
        ns,
        action,
        dataset_,
        resource,
        internal=True,
    )
    for model in models:
        yield from commands.getall(context, model, model.backend, **kwargs)


def traverse_ns_models(
    context: Context,
    ns: Namespace,
    action: Action,
    dataset_: Optional[str] = None,
    resource: Optional[str] = None,
    *,
    internal: bool = False,
):
    models = (ns.models or {})
    for model in models.values():
        if _model_matches_params(context, model, action, dataset_, resource, internal):
            yield model
    for ns_ in ns.names.values():
        if not internal and ns_.name.startswith('_'):
            continue
        yield from traverse_ns_models(
            context,
            ns_,
            action,
            dataset_,
            resource,
            internal=internal,
        )


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


def _get_ns_content(
    context: Context,
    request: Request,
    ns: Namespace,
    params: UrlParams,
    action: Action,
    *,
    recursive: bool = False,
    dataset_: Optional[str] = None,
    resource: Optional[str] = None,
) -> Response:
    if recursive:
        data = _get_ns_content_data_recursive(context, ns, action, dataset_, resource)
    else:
        data = _get_ns_content_data(context, ns, action, dataset_, resource)

    data = sorted(data, key=lambda x: (x.data['_type'] != 'ns', x.data['name']))

    model = ns.manifest.models['_ns']
    select = params.select or ['name', 'title', 'description']
    select_tree = get_select_tree(context, action, select)
    prop_names = get_select_prop_names(
        context,
        model,
        model.properties,
        action,
        select_tree,
        auth=False,
    )
    rows = (
        commands.prepare_data_for_response(
            context,
            model,
            params.fmt,
            row.data,
            action=action,
            select=select_tree,
            prop_names=prop_names,
        )
        for row in data
    )

    rows = log_response(context, rows)

    return render(context, request, model, params, rows, action=action)


class _NodeAndData(NamedTuple):
    node: Union[Namespace, Model]
    data: Dict[str, Any]


def _get_ns_content_data_recursive(
    context: Context,
    ns: Namespace,
    action: Action,
    dataset_: Optional[str] = None,
    resource: Optional[str] = None,
) -> Iterable[_NodeAndData]:
    yield from _get_ns_content_data(context, ns, action, dataset_, resource)
    for name in ns.names.values():
        yield from _get_ns_content_data_recursive(context, name, action, dataset_, resource)


def _get_ns_content_data(
    context: Context,
    ns: Namespace,
    action: Action,
    dataset_: Optional[str] = None,
    resource: Optional[str] = None,
) -> Iterable[_NodeAndData]:
    items: Iterable[Union[Namespace, Model]] = itertools.chain(
        ns.names.values(),
        ns.models.values(),
    )

    for item in items:
        if _model_matches_params(context, item, action, dataset_, resource):
            yield _NodeAndData(item, {
                '_type': item.node_type(),
                'name': item.model_type(),
                'title': item.title,
                'description': item.description,
            })


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
    models = traverse_ns_models(context, ns, Action.WIPE, internal=True)
    models = sort_models_by_refs(models)
    for model in models:
        if BackendFeatures.WRITE in model.backend.features:
            commands.wipe(context, model, model.backend)


def sort_models_by_refs(models: Iterable[Model]) -> Iterator[Model]:
    models = {model.model_type(): model for model in models}
    graph = collections.defaultdict(set)
    for name, model in models.items():
        graph[''].add(name)
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


def iter_model_refs(model: Model) -> Iterator[Ref]:
    for prop in model.properties.values():
        if prop.dtype.name == 'ref':
            yield prop.dtype
