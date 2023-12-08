from typing import Optional, NamedTuple, Union, Dict, Any, Iterable
from starlette.requests import Request
from starlette.responses import Response
from spinta import commands
from spinta.accesslog import log_response
from spinta.backends.helpers import get_select_tree, get_select_prop_names
from spinta.renderer import render
from spinta.compat import urlparams_to_expr
from spinta.components import Context, Namespace, Action, UrlParams, Model
from spinta.manifests.components import Manifest
from spinta.types.namespace import _model_matches_params, check_if_model_has_backend_and_source
from spinta.utils import itertools


@commands.traverse_ns_models.register(Context, Namespace, Manifest, Action)
def traverse_ns_models(
    context: Context,
    ns: Namespace,
    manifest: Manifest,
    action: Action,
    dataset_: Optional[str] = None,
    resource: Optional[str] = None,
    internal: bool = False,
    source_check: bool = False,
    **kwargs
):
    models = (ns.models or {})
    for model in models.values():
        if not (source_check and not check_if_model_has_backend_and_source(model)):
            if _model_matches_params(context, model, action, dataset_, resource, internal):
                yield model
    for ns_ in ns.names.values():
        if not internal and ns_.name.startswith('_'):
            continue
        yield from commands.traverse_ns_models(
            context,
            ns_,
            manifest,
            action,
            dataset_=dataset_,
            resource=resource,
            internal=internal,
            source_check=source_check
        )


@commands.getall.register(Context, Namespace, Request, Manifest)
def getall(
    context: Context,
    ns: Namespace,
    request: Request,
    manifest: Manifest,
    *,
    action: Action,
    params: UrlParams
):
    if params.all and params.ns:

        for model in commands.traverse_ns_models(context, ns, manifest, action, internal=True):
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
        for model in commands.traverse_ns_models(context, ns, manifest, action, internal=True):
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
        rows = commands.getall(context, ns, action=action, query=expr)
        rows = (
            commands.prepare_data_for_response(
                context,
                commands.get_model(context, manifest, row['_type']),
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

    model = commands.get_model(context, ns.manifest, '_ns')
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
