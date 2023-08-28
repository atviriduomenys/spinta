import uuid
from typing import overload, Optional, Iterator, Union, List, Tuple
from pathlib import Path

from starlette.requests import Request
from starlette.responses import Response
from starlette.responses import FileResponse

from spinta import commands
from spinta.backends.helpers import get_select_prop_names
from spinta.backends.helpers import get_select_tree
from spinta.backends.components import Backend
from spinta.compat import urlparams_to_expr
from spinta.components import Context, Node, Action, UrlParams, Page, ParamsPage
from spinta.components import Model
from spinta.components import Property
from spinta.core.ufuncs import Expr, asttoexpr
from spinta.datasets.components import ExternalBackend
from spinta.renderer import render
from spinta.types.datatype import Integer
from spinta.types.datatype import Object
from spinta.types.datatype import File
from spinta.accesslog import AccessLog
from spinta.accesslog import log_response
from spinta.exceptions import UnavailableSubresource
from spinta.exceptions import ItemDoesNotExist
from spinta.types.datatype import DataType
from spinta.typing import ObjectData
from spinta.ufuncs.helpers import merge_formulas
from spinta.utils.data import take


@commands.getall.register(Context, Model, Request)
async def getall(
    context: Context,
    model: Model,
    request: Request,
    *,
    action: Action,
    params: UrlParams,
) -> Response:
    commands.authorize(context, action, model)

    backend = model.backend

    if isinstance(backend, ExternalBackend):
        # XXX: `add_count` is a hack, because, external backends do not
        #      support it yet.
        expr = urlparams_to_expr(params, add_count=False)
    else:
        expr = urlparams_to_expr(params)

    accesslog: AccessLog = context.get('accesslog')
    accesslog.request(
        # XXX: Read operations does not have a transaction, but it
        #      is needed for loging.
        txn=str(uuid.uuid4()),
        model=model.model_type(),
        action=action.value,
    )

    if params.head:
        rows = []
    else:
        if not params.count and backend.paginated and model.page and model.page.by:
            if params.limit:
                rows = paginate(context, model, backend, params.page, expr, params.limit)
            else:
                rows = get_page(context, model, backend, params.page, expr)
        else:
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
        if action == Action.SEARCH:
            reserved = ['_type', '_id', '_revision', '_base']
        else:
            reserved = ['_type', '_id', '_revision']
        prop_names = get_select_prop_names(
            context,
            model,
            model.properties,
            action,
            select_tree,
            reserved=reserved,
            include_denorm_props=False,
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

    rows = log_response(context, rows)
    model.page.clear()

    return render(context, request, model, params, rows, action=action)


def get_page(
    context: Context,
    model: Model,
    backend: Backend,
    page: ParamsPage,
    expr: Expr,
) -> Iterator[ObjectData]:
    config = context.get('config')
    page_size = config.push_page_size
    if page:
        size = page.size or model.page.size or page_size or 1000
        model.page.update_values_from_params_page(page)
    else:
        size = model.page.size or page_size or 1000
    query = _get_pagination_sort_query(model, expr)

    count = 0
    depth = 0
    end_loop = False
    while not end_loop:
        page_query = _get_pagination_limit_query(size - count, query)
        page_query = _get_pagination_compare_query(model, page_query, depth)

        rows = commands.getall(context, model, backend, query=page_query)
        for row in rows:
            count += 1
            model.page.update_values_from_row(row)
            yield row

        if count < size and depth < len(model.page.by) - 1:
            depth += 1
            model.page.clear_till_depth(depth)
        else:
            end_loop = True


def paginate(
    context: Context,
    model: Model,
    backend: Backend,
    page: ParamsPage,
    expr: Optional[Expr],
    limit: Optional[int],
) -> Iterator[ObjectData]:
    config = context.get('config')
    page_size = config.push_page_size
    if page:
        size = page.size or model.page.size or page_size or 1000
        model.page.update_values_from_params_page(page)
    else:
        size = model.page.size or page_size or 1000
    query = _get_pagination_sort_query(model, expr)
    query = _get_pagination_select_query(model, query)

    true_count = 0
    count = 0
    depth = 0
    end_loop = False
    while not end_loop:
        page_query = _get_pagination_limit_query(size - count, query)
        page_query = _get_pagination_compare_query(model, page_query, depth)

        rows = commands.getall(context, model, backend, query=page_query)
        for row in rows:
            if limit and true_count >= limit:
                end_loop = True
                break

            count += 1
            true_count += 1
            model.page.update_values_from_row(row)
            yield row

        if not end_loop:
            if depth < len(model.page.by) - 1:
                depth += 1
                model.page.clear_till_depth(depth)
            elif limit and count >= size and true_count < limit:
                count = 0
                depth = 0
            else:
                end_loop = True


def _get_pagination_sort_query(
    model: Model,
    expr: Union[Expr, None],
) -> Union[Expr, None]:
    sort_by = model.page.by
    sort_args = []
    sort = {}

    for by, page_by in sort_by.items():
        if by.startswith('-'):
            name = 'negative'
        else:
            name = 'bind'
        sort_args.append(asttoexpr({
            'name': name,
            'args': [page_by.prop.name]
        }))

    if sort_args:
        sort = {
            'name': 'sort',
            'args': sort_args
        }
        sort = asttoexpr(sort)

    if expr and sort:
        updated, query = _update_expr_args(expr, 'sort', sort_args)
        if not updated:
            query = merge_formulas(query, sort)
    else:
        query = sort or expr or None
    return query


def _update_expr_args(
    expr: Expr,
    name: str,
    args: List,
    override: bool = False
) -> Tuple[bool, Expr]:
    updated = False
    expr.args = list(expr.args)
    if expr.name == name:
        if override:
            expr.args = args
            updated = True
        else:
            if expr.args:
                expr.args.extend(args)
                updated = True
    else:
        for i, arg in enumerate(expr.args):
            if isinstance(arg, Expr):
                updated, expr.args[i] = _update_expr_args(arg, name, args, override)
                if updated:
                    break
    expr.args = tuple(expr.args)
    return updated, expr


def _get_pagination_limit_query(
    size: int,
    expr: Union[Expr, None],
) -> Union[Expr, None]:
    limit = asttoexpr({
        'name': 'limit',
        'args': [size]
    })
    if expr:
        updated, query = _update_expr_args(expr, 'limit', [size], override=True)
        if not updated:
            query = merge_formulas(query, limit)
    else:
        query = limit
    return query


def _get_pagination_select_query(
    model: Model,
    expr: Union[Expr, None],
) -> Union[Expr, None]:
    if expr:
        select_args = []
        for page_by in model.page.by.values():
            select_args.append(asttoexpr({
                'name': 'bind',
                'args': [page_by.prop.name]
            }))
        if select_args:
            _, expr = _update_expr_args(expr, 'select', select_args)
    return expr


def _get_pagination_compare_query(
    model: Model,
    expr: Union[Expr, None],
    depth: int = 0
) -> Union[Expr, None]:
    compare = {}
    for i, (by, page_by) in enumerate(reversed(model.page.by.items())):
        eq_op = i > depth
        if by.startswith('-'):
            if eq_op:
                op = 'ne'
            else:
                op = 'lt'
        else:
            if eq_op:
                op = 'eq'
            else:
                op = 'gt'

        if page_by.value:
            if compare:
                compare = {
                    'name': 'and',
                    'args': [
                        compare,
                        {
                            'name': op,
                            'args': [{
                                'name': 'bind',
                                'args': [page_by.prop.name]
                            }, page_by.value]
                        }
                    ]
                }
            else:
                compare = {
                    'name': op,
                    'args': [{
                        'name': 'bind',
                        'args': [page_by.prop.name]
                    }, page_by.value]
                }
    if compare:
        compare = asttoexpr(compare)

    if expr and compare:
        query = merge_formulas(expr, compare)
    else:
        query = compare or expr or None
    return query


@overload
@commands.getone.register(Context, Request, Node)
async def getone(
    context: Context,
    request: Request,
    node: Node,
    *,
    action: Action,
    params: UrlParams,
) -> Response:
    if params.prop and params.propref:
        resp = await commands.getone(
            context,
            request,
            params.prop,
            params.prop.dtype,
            params.model.backend,
            action=action,
            params=params,
        )
    elif params.prop:
        resp = await commands.getone(
            context,
            request,
            params.prop,
            params.prop.dtype,
            params.prop.dtype.backend or params.model.backend,
            action=action,
            params=params,
        )
    else:
        resp = await commands.getone(
            context,
            request,
            params.model,
            params.model.backend,
            action=action,
            params=params,
        )

    accesslog: AccessLog = context.get('accesslog')
    accesslog.response(objects=1)

    return resp


@overload
@commands.getone.register(Context, Request, Property, DataType, Backend)
async def getone(
    context: Context,
    request: Request,
    prop: Property,
    dtype: DataType,
    backend: Backend,
    *,
    action: Action,
    params: UrlParams,
) -> Response:
    raise UnavailableSubresource(prop=prop.name, prop_type=prop.dtype.name)


@overload
@commands.getone.register(Context, Request, Model, Backend)
async def getone(
    context: Context,
    request: Request,
    model: Model,
    backend: Backend,
    *,
    action: Action,
    params: UrlParams,
) -> Response:
    commands.authorize(context, action, model)

    accesslog: AccessLog = context.get('accesslog')
    accesslog.request(
        # XXX: Read operations does not have a transaction, but it
        #      is needed for loging.
        txn=str(uuid.uuid4()),
        model=model.model_type(),
        action=action.value,
        id_=params.pk,
    )

    data = commands.getone(context, model, backend, id_=params.pk)
    select_tree = get_select_tree(context, action, params.select)
    prop_names = get_select_prop_names(
        context,
        model,
        model.properties,
        action,
        select_tree,
    )
    data = commands.prepare_data_for_response(
        context,
        model,
        params.fmt,
        data,
        action=action,
        select=select_tree,
        prop_names=prop_names,
    )
    return render(context, request, model, params, data, action=action)


@overload
@commands.getone.register(Context, Request, Property, Object, Backend)
async def getone(
    context: Context,
    request: Request,
    prop: Property,
    dtype: Object,
    backend: Backend,
    *,
    action: Action,
    params: UrlParams,
) -> Response:
    commands.authorize(context, action, prop)

    accesslog: AccessLog = context.get('accesslog')
    accesslog.request(
        # XXX: Read operations does not have a transaction, but it
        #      is needed for loging.
        txn=str(uuid.uuid4()),
        model=prop.model.model_type(),
        prop=prop.place,
        action=action.value,
        id_=params.pk,
    )

    data = commands.getone(
        context,
        prop,
        dtype,
        backend,
        id_=params.pk,
    )

    data = commands.prepare_data_for_response(
        context,
        prop.dtype,
        params.fmt,
        data,
        action=action,
    )
    return render(context, request, prop, params, data, action=action)


@overload
@commands.getone.register(Context, Request, Property, File, Backend)
async def getone(
    context: Context,
    request: Request,
    prop: Property,
    dtype: File,
    backend: Backend,
    *,
    action: Action,
    params: UrlParams,
) -> Response:
    commands.authorize(context, action, prop)

    accesslog: AccessLog = context.get('accesslog')
    accesslog.request(
        # XXX: Read operations does not have a transaction, but it
        #      is needed for loging.
        txn=str(uuid.uuid4()),
        model=prop.model.model_type(),
        prop=prop.place,
        action=action.value,
        id_=params.pk,
    )

    # Get metadata from model backend
    data = commands.getone(
        context,
        prop,
        dtype,
        prop.model.backend,
        id_=params.pk,
    )

    # Return file metadata from model backend
    if params.propref:
        data = commands.prepare_data_for_response(
            context,
            prop.dtype,
            params.fmt,
            data,
            action=Action.GETONE,
        )
        return render(context, request, prop, params, data, action=action)

    # Return file content from property backend
    else:
        value = take(prop.place, data)

        file = commands.getfile(
            context,
            prop,
            dtype,
            dtype.backend,
            data=value,
        )

        if file is None:
            raise ItemDoesNotExist(dtype, id=params.pk)

        filename = value['_id']

        if isinstance(file, bytes):
            ResponseClass = Response
        elif isinstance(file, Path):
            ResponseClass = FileResponse
        else:
            raise ValueError(f"Unknown file type {type(file)}.")

        return ResponseClass(
            file,
            media_type=value.get('_content_type'),
            headers={
                'Revision': data['_revision'],
                'Content-Disposition': (
                    f'attachment; filename="{filename}"'
                    if filename else
                    'attachment'
                )
            },
        )


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

    if params.head:
        rows = []
    else:
        accesslog = context.get('accesslog')
        accesslog.request(
            # XXX: Read operations does not have a transaction, but it
            #      is needed for loging.
            txn=str(uuid.uuid4()),
            model=model.model_type(),
            action=action.value,
        )

        rows = commands.changes(
            context,
            model,
            model.backend,
            id_=params.pk,
            limit=params.limit,
            offset=params.changes_offset,
        )

    select_tree = get_select_tree(context, action, params.select)
    prop_names = get_select_prop_names(
        context,
        model,
        model.properties,
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


@commands.summary.register(Context, Request, Model)
async def summary(
    context: Context,
    request: Request,
    model: Model,
    *,
    action: Action,
    params: UrlParams,
) -> Response:
    commands.authorize(context, action, model)
    backend = model.backend
    prop = params.select[0]

    accesslog: AccessLog = context.get('accesslog')
    accesslog.request(
        # XXX: Read operations does not have a transaction, but it
        #      is needed for loging.
        txn=str(uuid.uuid4()),
        model=model.model_type(),
        action=action.value,
    )
    if params.head:
        rows = []
    else:
        args = {
            "prop": prop
        }
        if params.query:
            for item in params.query:
                args[item["name"]] = item["args"]
        rows = commands.summary(context, model, backend, args=args)
    rows = log_response(context, rows)
    return render(context, request, model, params, rows, action=action)
