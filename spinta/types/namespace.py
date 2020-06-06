from typing import Optional, Iterable, Iterator

import collections
import itertools

from toposort import toposort

from starlette.requests import Request
from starlette.responses import Response

from spinta import commands
from spinta.components import Context, UrlParams, Namespace, Action, Model, Property, Node
from spinta.renderer import render
from spinta.nodes import load_node, load_model_properties
from spinta import exceptions
from spinta.auth import authorized
from spinta.compat import urlparams_to_expr


@commands.link.register(Context, Namespace)
def link(context: Context, ns: Namespace):
    pass


@commands.check.register(Context, Namespace)
def check(context: Context, ns: Namespace):
    pass


@commands.authorize.register(Context, Action, Namespace)
def authorize(context: Context, action: Action, ns: Namespace):
    authorized(context, ns, action, throw=True)


@commands.getall.register(Context, Request, Namespace, type(None))
async def getall(
    context: Context,
    request: Request,
    ns: Namespace,
    backend: type(None),
    *,
    action: Action,
    params: UrlParams,
) -> Response:
    commands.authorize(context, action, ns)
    if params.all and params.ns:
        for model in traverse_ns_models(context, ns, action):
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
        for model in traverse_ns_models(context, ns, action):
            commands.authorize(context, action, model)
        expr = urlparams_to_expr(params)
        data = getall(context, ns, None, action=action, query=expr)
        return render(context, request, ns, params, data, action=action)
    else:
        return _get_ns_content(context, request, ns, params, action)


@commands.getall.register(Context, Namespace, type(None))
def getall(
    context: Context,
    ns: Namespace,
    backend: type(None),
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
    for model in traverse_ns_models(context, ns, action, dataset_, resource):
        yield from commands.getall(context, model, model.backend, **kwargs)


def traverse_ns_models(
    context: Context,
    ns: Namespace,
    action: Action,
    dataset_: Optional[str] = None,
    resource: Optional[str] = None,
):
    for model in (ns.models or {}).values():
        if _model_matches_params(context, model, action, dataset_, resource):
            yield model
    for name in ns.names.values():
        yield from traverse_ns_models(context, name, action, dataset_, resource)


def _model_matches_params(
    context: Context,
    model: Model,
    action: Action,
    dataset_: Optional[str] = None,
    resource: Optional[str] = None,
):
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
    data = sorted(data, key=lambda x: (x['_type'] != 'ns', x['_id']))

    schema = {
        'type': 'model:ns',
        'name': ns.model_type(),
        'properties': {
            '_type': {
                'type': 'string',
                'access': ns.access.value,
            },
            '_id': {
                'type': 'pk',
                'access': ns.access.value,
            },
            'title': {
                'type': 'string',
                'access': ns.access.value,
            },
        }
    }
    model = Model()
    model.eid = None
    model.ns = ns
    model.parent = ns.manifest  # XXX: deprecated
    model.manifest = ns.manifest
    load_node(context, model, schema)
    load_model_properties(context, model, Property, schema['properties'])

    return render(context, request, model, params, data, action=action)


def _get_ns_content_data_recursive(
    context: Context,
    ns: Namespace,
    action: Action,
    dataset_: Optional[str] = None,
    resource: Optional[str] = None,
) -> Iterable[dict]:
    yield from _get_ns_content_data(context, ns, action, dataset_, resource)
    for name in ns.names.values():
        yield from _get_ns_content_data_recursive(context, name, action, dataset_, resource)
        yield from _get_ns_content_data(context, name, action, dataset_, resource)


def _get_ns_content_data(
    context: Context,
    ns: Namespace,
    action: Action,
    dataset_: Optional[str] = None,
    resource: Optional[str] = None,
) -> Iterable[dict]:
    models = itertools.chain(
        ns.names.values(),
        ns.models.values(),
    )

    for model in models:
        if _model_matches_params(context, model, action, dataset_, resource):
            yield {
                '_type': model.node_type(),
                '_id': model.model_type(),
                'title': model.title,
            }


@commands.getone.register(Context, Request, Namespace)
async def getone(
    context: Context,
    request: Request,
    ns: Namespace,
    *,
    action: Action,
    params: UrlParams,
):
    raise exceptions.ModelNotFound(model=ns.name)


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
    models = traverse_ns_models(context, ns, Action.WIPE)
    models = sort_models_by_refs(models)
    for model in models:
        commands.wipe(context, model, model.backend)


def sort_models_by_refs(models: Iterable[Model]) -> Iterator[Model]:
    models = {model.model_type(): model for model in models}
    graph = collections.defaultdict(set)
    for name, model in models.items():
        graph[''].add(name)
        for prop in iter_model_refs(model):
            ref = prop.dtype.model.model_type()
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


def iter_model_refs(model: Model):
    for prop in model.properties.values():
        if prop.dtype.name == 'ref':
            yield prop
