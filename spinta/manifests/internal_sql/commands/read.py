from typing import Optional

from starlette.requests import Request
from starlette.responses import Response
from spinta import commands
from spinta.accesslog import log_response
from spinta.backends.helpers import get_select_tree, get_select_prop_names
from spinta.compat import urlparams_to_expr
from spinta.manifests.internal_sql.commands.manifest import get_model_name_list
from spinta.renderer import render
from spinta.components import Context, Namespace, Action, UrlParams
from spinta.manifests.internal_sql.components import InternalSQLManifest
from spinta.manifests.internal_sql.helpers import get_namespace_partial_data
from spinta.types.namespace import check_if_model_has_backend_and_source, _model_matches_params


@commands.traverse_ns_models.register(Context, Namespace, InternalSQLManifest, Action)
def traverse_ns_models(
    context: Context,
    ns: Namespace,
    manifest: InternalSQLManifest,
    action: Action,
    dataset_: Optional[str] = None,
    resource: Optional[str] = None,
    internal: bool = False,
    source_check: bool = False,
    loaded: bool = False,
    **kwargs
):
    models = get_model_name_list(context, manifest, loaded, namespace=ns.name, recursive=True)
    for model_name in models:
        model = commands.get_model(context, manifest, model_name)
        if not (source_check and not check_if_model_has_backend_and_source(model)):
            if _model_matches_params(context, model, action, dataset_, resource, internal):
                yield model


@commands.getall.register(Context, Namespace, Request, InternalSQLManifest)
def getall(
    context: Context,
    ns: Namespace,
    request: Request,
    manifest: InternalSQLManifest,
    *,
    action: Action,
    params: UrlParams
):
    if params.all and params.ns:
        # for model in traverse_ns_models(context, ns, action, internal=True):
        #     commands.authorize(context, action, model)
        return _get_internal_ns_content(
            context,
            request,
            ns,
            manifest,
            params,
            action,
            recursive=True
        )
    elif params.all:
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
        return _get_internal_ns_content(
            context,
            request,
            ns,
            manifest,
            params,
            action
        )


def _get_internal_ns_content(
    context: Context,
    request: Request,
    ns: Namespace,
    manifest: InternalSQLManifest,
    params: UrlParams,
    action: Action,
    *,
    recursive: bool = False,
) -> Response:
    parents = [parent.name for parent in ns.parents()]
    partial_data = get_namespace_partial_data(context, manifest, ns.name, parents=parents, recursive=recursive, action=action)

    data = sorted(partial_data, key=lambda x: (x['_type'] != 'ns', x['name']))
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
            row,
            action=action,
            select=select_tree,
            prop_names=prop_names,
        )
        for row in data
    )

    rows = log_response(context, rows)

    return render(context, request, model, params, data=rows, action=action)
