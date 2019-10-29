from typing import Union, Optional

import itertools

from starlette.requests import Request
from starlette.responses import Response

from spinta import commands
from spinta.components import Context, UrlParams, Namespace, Action, Model, Property, Node
from spinta.renderer import render
from spinta.nodes import load_node, load_model_properties
from spinta import exceptions
from spinta.types import dataset


@commands.check.register()
def check(context: Context, ns: Namespace):
    pass


@commands.getall.register()
async def getall(
    context: Context,
    request: Request,
    ns: Namespace,
    backend: type(None),
    *,
    action: Action,
    params: UrlParams,
) -> Response:
    if params.all:
        for model in _traverse_ns_models(ns, params.dataset, params.resource, params.origin):
            commands.authorize(context, action, model)
        data = getall(
            context, ns, None,
            action=action,
            dataset_=params.dataset,
            resource=params.resource,
            origin=params.origin,
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


@commands.getall.register()
@getall.register()  # noqa
def getall(
    context: Context,
    ns: Namespace,
    backend: type(None),
    *,
    action: Optional[Action] = None,
    dataset_: Optional[str] = None,
    resource: Optional[str] = None,
    origin: Optional[str] = None,
    **kwargs
):
    return _query_data(context, ns, action, dataset_, resource, origin, **kwargs)


def _query_data(
    context: Context,
    ns: Namespace,
    action: Action,
    dataset_: Optional[str] = None,
    resource: Optional[str] = None,
    origin: Optional[str] = None,
    **kwargs,
):
    for model in _traverse_ns_models(ns, dataset_, resource, origin):
        yield from commands.getall(
            context, model, model.backend,
            action=action,
            **kwargs
        )


def _traverse_ns_models(
    ns: Namespace,
    dataset_: Optional[str] = None,
    resource: Optional[str] = None,
    origin: Optional[str] = None,
):
    for model in (ns.models or {}).values():
        if _model_matches_params(model, dataset_, resource, origin):
            yield model
    for name in ns.names.values():
        yield from _traverse_ns_models(name, dataset_, resource, origin)


def _model_matches_params(
    model: Union[Model, dataset.Model],
    dataset_: Optional[str] = None,
    resource: Optional[str] = None,
    origin: Optional[str] = None,
):
    if (
        dataset_ is None and
        resource is None and
        origin is None
    ):
        return True

    if not isinstance(model, dataset.Model):
        return False

    if (
        dataset_ is not None and
        dataset_ != model.parent.parent.name
    ):
        return False

    if (
        resource is not None and
        resource != model.parent.name
    ):
        return False

    if (
        origin is not None and
        origin != model.origin
    ):
        return False

    return True


def _get_ns_content(
    context: Context,
    request: Request,
    ns: Namespace,
    params: UrlParams,
    action: Action,
) -> Response:
    data = [
        {
            '_type': model.node_type(),
            '_id': model.model_type(),
            'name': model.name,
            'specifier': model.model_specifier(),
            'title': model.title,
        }
        for model in itertools.chain(
            ns.names.values(),
            ns.models.values(),
        )
    ]
    data = sorted(data, key=lambda x: (x['_type'] != 'ns', x['_id']))

    schema = {
        'path': __file__,
        'parent': ns.manifest,
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
    model = load_node(context, Model(), schema, ns.manifest)
    load_model_properties(context, model, Property, schema['properties'])

    return render(context, request, model, params, data, action=action)


@commands.getone.register()
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
def in_namespace(node: Node, parent: Node) -> bool:
    while node:
        if node is parent:
            return True
        node = node.parent
    return False
