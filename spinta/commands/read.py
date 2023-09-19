import uuid
from copy import deepcopy
from typing import overload, Optional, Iterator, List, Tuple
from pathlib import Path

from starlette.requests import Request
from starlette.responses import Response
from starlette.responses import FileResponse

from spinta import commands
from spinta.backends.helpers import get_select_prop_names
from spinta.backends.helpers import get_select_tree
from spinta.backends.components import Backend
from spinta.backends.nobackend.components import NoBackend
from spinta.compat import urlparams_to_expr
from spinta.components import Context, Node, Action, UrlParams, Page, PageBy
from spinta.components import Model
from spinta.components import Property
from spinta.core.ufuncs import Expr
from spinta.datasets.components import ExternalBackend
from spinta.renderer import render
from spinta.types.datatype import Integer
from spinta.types.datatype import Object
from spinta.types.datatype import File
from spinta.accesslog import AccessLog
from spinta.accesslog import log_response
from spinta.exceptions import UnavailableSubresource, InfiniteLoopWithPagination, DuplicateRowWhilePaginating, BackendNotGiven
from spinta.exceptions import ItemDoesNotExist
from spinta.types.datatype import DataType
from spinta.typing import ObjectData
from spinta.ufuncs.basequerybuilder.components import get_allowed_page_property_types
from spinta.ufuncs.basequerybuilder.ufuncs import add_page_expr
from spinta.utils.data import take
from spinta.utils.encoding import encode_page_values


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
    if isinstance(backend, NoBackend):
        raise BackendNotGiven(model)

    copy_page = prepare_page_for_get_all(context, model, params)
    is_page_enabled = not params.count and backend.paginated and copy_page and copy_page and copy_page.is_enabled

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
        if is_page_enabled:
            rows = get_page(context, model, backend, copy_page, expr, params.limit)
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
                for key, val in row.items() if key in props
            }
            for row in rows
        )
    else:
        select_tree = get_select_tree(context, action, params.select)
        if action == Action.SEARCH:
            reserved = ['_type', '_id', '_revision', '_base']
        else:
            reserved = ['_type', '_id', '_revision']
        if model.page.is_enabled:
            reserved.append('_page')
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

    return render(context, request, model, params, rows, action=action)


# XXX: params.sort handle should be moved to BaseQueryBuilder, AST should be handled by Env, this only supports basic sort arguments, that ar Positive, Negative or Bind.
# Issue: in order to create correct WHERE, we need to have finalized Page, currently we combine sort properties with page here.
def prepare_page_for_get_all(context: Context, model: Model, params: UrlParams):
    if model.page:
        copied = deepcopy(model.page)
        copied.clear()

        if params.page:
            copied.is_enabled = params.page.is_enabled
        if copied.is_enabled:
            config = context.get('config')
            page_size = config.push_page_size
            size = params.page and params.page.size or copied.size or page_size or 1000
            copied.size = size

            if params.sort:
                new_order = {}
                for sort_ast in params.sort:
                    negative = False
                    if sort_ast['name'] == 'negative':
                        negative = True
                        sort_ast = sort_ast['args'][0]
                    elif sort_ast['name'] == 'positive':
                        sort_ast = sort_ast['args'][0]
                    for sort in sort_ast['args']:
                        if isinstance(sort, str):
                            if sort in model.properties.keys():
                                by = sort if not negative else f'-{sort}'
                                prop = model.properties[sort]
                                if isinstance(prop.dtype, get_allowed_page_property_types()):
                                    new_order[by] = PageBy(prop)
                                else:
                                    copied.is_enabled = False
                                    break
                        else:
                            copied.is_enabled = False
                            break

                for key, page_by in copied.by.items():
                    reversed_key = key[1:] if key.startswith("-") else f'-{key}'
                    if key not in new_order and reversed_key not in new_order:
                        new_order[key] = page_by

                copied.by = new_order

            if params.page and params.page.values:
                copied.update_values_from_list(params.page.values)

        return copied


def get_page(
    context: Context,
    model: Model,
    backend: Backend,
    model_page: Page,
    expr: Expr,
    limit: Optional[int] = None
) -> Iterator[ObjectData]:
    config = context.get('config')
    page_size = config.push_page_size
    model_page.size = model_page.size or page_size or 1000

    true_count = 0

    last_value = None
    while True:
        finished = True
        query = add_page_expr(expr, model_page)
        rows = commands.getall(context, model, backend, query=query)
        first_value = None
        previous_value = None
        for row in rows:
            if previous_value is not None:
                if previous_value == row:
                    raise DuplicateRowWhilePaginating(key=encode_page_values(row['_page']))
            previous_value = row
            if finished:
                finished = False

            if limit and true_count >= limit:
                finished = True
                break

            if first_value is None:
                first_value = row
                if first_value == last_value:
                    raise InfiniteLoopWithPagination()
                else:
                    last_value = first_value

            true_count += 1
            if '_page' in row:
                model_page.update_values_from_list(row['_page'])
            yield row

        if finished:
            break


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
