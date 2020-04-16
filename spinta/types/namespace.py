from typing import Optional, Iterable

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


@commands.link.register(Context, Namespace)
def link(context: Context, ns: Namespace):
    pass


@commands.check.register(Context, Namespace)
def check(context: Context, ns: Namespace):
    pass


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
    if params.all and params.ns:
        for model in traverse_ns_models(ns):
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
        for model in traverse_ns_models(ns):
            commands.authorize(context, action, model)
        data = getall(
            context, ns, None,
            action=action,
            select=params.select,
            sort=params.sort,
            offset=params.offset,
            limit=params.limit,
            count=params.count,
            query=params.query,
        )
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
    for model in traverse_ns_models(ns, dataset_, resource):
        yield from commands.getall(context, model, model.backend, **kwargs)


def traverse_ns_models(
    ns: Namespace,
    dataset_: Optional[str] = None,
    resource: Optional[str] = None,
):
    for model in (ns.models or {}).values():
        if _model_matches_params(model, dataset_, resource):
            yield model
    for name in ns.names.values():
        yield from traverse_ns_models(name, dataset_, resource)


def _model_matches_params(
    model: Model,
    dataset_: Optional[str] = None,
    resource: Optional[str] = None,
):
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
        data = _get_ns_content_data_recursive(ns, dataset_, resource)
    else:
        data = _get_ns_content_data(ns, dataset_, resource)
    data = sorted(data, key=lambda x: (x['_type'] != 'ns', x['_id']))

    schema = {
        'type': 'model:ns',
        'name': ns.model_type(),
        'properties': {
            '_type': {'type': 'string'},
            '_id': {'type': 'pk'},
            'name': {'type': 'string'},
            'specifier': {'type': 'string'},
            'title': {'type': 'string'},
        }
    }
    model = Model()
    model.path = None
    model.parent = ns.manifest  # XXX: deprecated
    model.manifest = ns.manifest
    load_node(context, model, schema)
    load_model_properties(context, model, Property, schema['properties'])

    return render(context, request, model, params, data, action=action)


def _get_ns_content_data_recursive(
    ns: Namespace,
    dataset_: Optional[str] = None,
    resource: Optional[str] = None,
) -> Iterable[dict]:
    yield from _get_ns_content_data(ns, dataset_, resource)
    for name in ns.names.values():
        yield from _get_ns_content_data_recursive(name, dataset_, resource)
        yield from _get_ns_content_data(name, dataset_, resource)


def _get_ns_content_data(
    ns: Namespace,
    dataset_: Optional[str] = None,
    resource: Optional[str] = None,
) -> Iterable[dict]:
    models = itertools.chain(
        ns.names.values(),
        ns.models.values(),
    )

    for model in models:
        if _model_matches_params(model, dataset_, resource):
            yield {
                '_type': model.node_type(),
                '_id': model.model_type(),
                'name': model.name,
                'specifier': model.model_specifier(),
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


@commands.wipe.register()
def wipe(context: Context, ns: Namespace, backend: type(None)):
    models = {
        model.model_type(): model
        for model in traverse_ns_models(ns)
    }

    graph = collections.defaultdict(set)
    for name, model in models.items():
        if name not in graph:
            graph[name] = set()
        for prop in model.properties.values():
            if prop.dtype.name == 'ref':
                ref_model = commands.get_referenced_model(
                    context, prop, prop.dtype.object
                )
                graph[ref_model.model_type()].add(name)

    for names in toposort(graph):
        for name in names:
            model = models[name]
            commands.wipe(context, model, model.backend)
