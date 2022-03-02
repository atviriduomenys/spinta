from starlette.requests import Request

from spinta import commands
from spinta.backends.helpers import get_select_prop_names
from spinta.backends.helpers import get_select_tree
from spinta.compat import urlparams_to_expr
from spinta.components import Context, Node, Action, UrlParams
from spinta.components import Model
from spinta.components import Property
from spinta.datasets.components import ExternalBackend
from spinta.renderer import render
from spinta.types.datatype import Integer


@commands.getall.register(Context, Model, Request)
async def getall(
    context: Context,
    model: Model,
    request: Request,
    *,
    action: Action,
    params: UrlParams,
):
    commands.authorize(context, action, model)

    backend = model.backend

    if isinstance(backend, ExternalBackend):
        # XXX: `add_count` is a hack, because, external backends do not
        #      support it yet.
        expr = urlparams_to_expr(params, add_count=False)
    else:
        expr = urlparams_to_expr(params)

    accesslog = context.get('accesslog')
    accesslog.log(
        model=model.model_type(),
        action=action.value,
    )

    rows = commands.getall(context, model, backend, query=expr)

    if params.count:
        # XXX: Quick and dirty hack. Functions should be handled properly.
        prop = Property()
        prop.name = 'count()'
        prop.place = 'count()'
        prop.title = ''
        prop.description = ''
        prop.model = model
        prop.dtype = Integer()
        prop.dtype.type = 'integer'
        prop.dtype.type_args = []
        prop.dtype.name = 'integer'
        prop.dtype.prop = prop
        props = {
            '_type': model.properties['_type'],
            'count()': prop,
        }
        rows = (
            {
                prop.name: commands.prepare_dtype_for_response(
                    context,
                    params.fmt,
                    props[key].dtype,
                    val,
                    data=row,
                    action=action,
                )
                for key, val in row.items()
            }
            for row in rows
        )
    else:
        select_tree = get_select_tree(context, action, params.select)
        prop_names = get_select_prop_names(
            context,
            model,
            action,
            select_tree,
            reserved=['_type', '_id', '_revision'],
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
            for row in rows
        )
    return render(context, request, model, params, rows, action=action)


@commands.getone.register(Context, Request, Node)
async def getone(
    context: Context,
    request: Request,
    node: Node,
    *,
    action: Action,
    params: UrlParams,
):
    if params.prop and params.propref:
        return await commands.getone(context, request, params.prop, params.model.backend, action=action, params=params)
    elif params.prop:
        return await commands.getone(context, request, params.prop, params.prop.dtype.backend or params.model.backend, action=action, params=params)
    else:
        return await commands.getone(context, request, params.model, params.model.backend, action=action, params=params)


@commands.changes.register(Context, Model, Request)
async def changes(
    context: Context,
    model: Model,
    request: Request,
    *,
    action: Action,
    params: UrlParams,
):
    commands.authorize(context, action, model)
    rows = commands.changes(
        context,
        model,
        model.backend,
        id_=params.pk,
        limit=params.limit,
        offset=params.offset,
    )
    select_tree = get_select_tree(context, action, params.select)
    prop_names = get_select_prop_names(
        context,
        model,
        action,
        select_tree,
        reserved=[
            '_cid',
            '_created',
            '_op',
            '_id',
            '_txn',
            '_revision',
        ],
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
        for row in rows
    )
    return render(context, request, model, params, rows, action=action)
