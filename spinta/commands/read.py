import uuid
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import overload, Optional, Iterator, List, Tuple, Callable

from starlette.requests import Request
from starlette.responses import FileResponse
from starlette.responses import Response

from spinta import commands
from spinta.accesslog import AccessLog
from spinta.accesslog import log_response
from spinta.backends.components import Backend
from spinta.backends.helpers import get_select_prop_names
from spinta.backends.helpers import get_select_tree
from spinta.backends.nobackend.components import NoBackend
from spinta.compat import urlparams_to_expr
from spinta.components import Context, Node, Action, UrlParams, Page, PageBy, get_page_size, Config, \
    pagination_enabled
from spinta.components import Model
from spinta.components import Property
from spinta.core.ufuncs import Expr
from spinta.exceptions import ItemDoesNotExist
from spinta.exceptions import UnavailableSubresource, InfiniteLoopWithPagination, BackendNotGiven, TooShortPageSize, \
    TooShortPageSizeKeyRepetition
from spinta.renderer import render
from spinta.types.datatype import DataType
from spinta.types.datatype import File
from spinta.types.datatype import Object
from spinta.typing import ObjectData
from spinta.ufuncs.basequerybuilder.components import QueryParams, QueryPage
from spinta.ufuncs.basequerybuilder.helpers import update_query_with_url_params, add_page_expr
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
    if isinstance(backend, NoBackend):
        raise BackendNotGiven(model)

    copy_page = commands.create_page(model.page, params)

    is_page_enabled = copy_page and copy_page and copy_page.enabled
    expr = urlparams_to_expr(params)
    query_params = QueryParams()
    update_query_with_url_params(query_params, params)
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
            rows = get_page(context, model, backend, copy_page, expr, params.limit, default_expand=False, params=query_params)
        else:
            rows = commands.getall(context, model, backend, params=query_params, query=expr, default_expand=False)

    rows = prepare_data_for_response(
        context,
        model,
        action,
        params,
        rows,
    )

    rows = log_response(context, rows)

    return render(context, request, model, params, rows, action=action)


def prepare_data_for_response(
    context: Context,
    model: Model,
    action: Action,
    params: UrlParams,
    rows,
    reserved: List[str] = None,
):
    if isinstance(rows, dict):
        rows = [rows]

    prop_select = params.select_props
    func_select = params.select_funcs

    prop_select_tree = get_select_tree(context, action, prop_select)
    func_select_tree = get_select_tree(context, action, func_select)

    if reserved is None:
        if action == Action.SEARCH:
            reserved = ['_type', '_id', '_revision', '_base']
        else:
            reserved = ['_type', '_id', '_revision']
        if pagination_enabled(model, params):
            reserved.append('_page')
    prop_names = get_select_prop_names(
        context,
        model,
        model.properties,
        action,
        prop_select_tree,
        reserved=reserved,
        include_denorm_props=False,
    )

    for row in rows:
        result = commands.prepare_data_for_response(
            context,
            model,
            params.fmt,
            row,
            action=action,
            select=prop_select_tree,
            prop_names=prop_names,
        )
        if func_select:
            for key, func_prop in func_select.items():
                result[key] = commands.prepare_dtype_for_response(
                    context,
                    params.fmt,
                    func_prop.prop.dtype,
                    row[key],
                    data=row,
                    action=action,
                    select=func_select_tree
                )
        yield result


def _is_iter_last_real_value(it: int, total: int, added_size: int = 1):
    return it > (total - 1 - added_size)


def _is_iter_last_potential_value(it: int, total: int):
    return it > (total - 1)


def get_page(
    context: Context,
    model: Model,
    backend: Backend,
    model_page: Page,
    expr: Expr,
    limit: Optional[int] = None,
    default_expand: bool = True,
    params: QueryParams = None,
) -> Iterator[ObjectData]:
    config = context.get('config')
    size = get_page_size(config, model, model_page)

    # Add 1 to see future value (to see if it finished, check for infinite loops and page size miss matches).
    model_page.size = size + 1

    page_meta = PaginationMetaData(
        page_size=size,
        limit=limit
    )
    while not page_meta.is_finished:
        page_meta.is_finished = True
        query = add_page_expr(expr, model_page)
        rows = commands.getall(context, model, backend, params=params, query=query, default_expand=default_expand)

        yield from get_paginated_values(model_page, page_meta, rows, extract_source_page_keys)


def extract_source_page_keys(row: dict):
    if '_page' in row:
        return row['_page']
    return []


@dataclass
class PaginationMetaData:
    page_size: int
    previous_first_value = None
    is_first_iter = True
    is_finished = False
    true_count = 0
    limit: int = None

    def handle_count(self) -> bool:
        if self.limit:
            if self.true_count >= self.limit:
                self.is_finished = True
                return True
            self.true_count += 1
        return False


def get_paginated_values(model_page: Page, meta: PaginationMetaData, rows, extract_page_keys: Callable):
    size = meta.page_size

    if model_page.first_time != meta.is_first_iter:
        model_page.first_time = meta.is_first_iter
    if meta.is_first_iter:
        meta.is_first_iter = False

    initial_key = [val.value for val in model_page.by.values()]
    current_first_value = None
    previous_value = None
    key_repetition = [[], 0]
    flag_for_potential_key_repetition = False
    for i, row in enumerate(rows):
        keys = extract_page_keys(row).copy()
        # Check if future value is not the same as last value
        if key_repetition[0] == keys:
            key_repetition[1] += 1
        else:
            key_repetition = [keys, 0]

        if _is_iter_last_potential_value(i, size):
            meta.is_finished = False
            if flag_for_potential_key_repetition:
                raise TooShortPageSize(
                    model_page,
                    page_size=size,
                    page_values=previous_value
                )
            if key_repetition[1] > 0:
                raise TooShortPageSizeKeyRepetition(
                    model_page,
                    page_size=size,
                    page_values=previous_value,
                )
            break

        previous_value = row

        limit_result = meta.handle_count()
        if limit_result:
            break

        # Check if row is completely the same as previous page first row
        if current_first_value is None:
            current_first_value = row
            if current_first_value == meta.previous_first_value:
                raise InfiniteLoopWithPagination(
                    model_page,
                    page_size=size,
                    page_values=current_first_value
                )
            meta.previous_first_value = current_first_value

        model_page.update_values_from_list(keys)

        # Check if initial key is the same as last key
        if _is_iter_last_real_value(i, size):
            last_key = keys
            if initial_key == last_key:
                flag_for_potential_key_repetition = True

        yield row


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
    data = next(prepare_data_for_response(context, model, action, params, data, reserved=[]))
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

    rows = prepare_data_for_response(context, model, action, params, rows, reserved=[
        '_cid',
        '_created',
        '_op',
        '_id',
        '_txn',
        '_revision',
    ])

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
        expr = urlparams_to_expr(params)
        rows = commands.summary(context, model, backend, expr)
    rows = log_response(context, rows)
    return render(context, request, model, params, rows, action=action)
