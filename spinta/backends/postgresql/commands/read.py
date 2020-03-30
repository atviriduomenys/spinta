from typing import Optional, List, Union, Tuple, Iterator, Dict

import sqlalchemy as sa

from multipledispatch import dispatch
from sqlalchemy.dialects.postgresql import UUID

from starlette.requests import Request
from starlette.responses import Response

from spinta import commands
from spinta import exceptions
from spinta.renderer import render
from spinta.components import Context, Model, Property, Action, UrlParams
from spinta.backends.components import BackendFeatures
from spinta.backends import log_getall, log_getone
from spinta.backends.postgresql.files import DatabaseFile
from spinta.hacks.recurse import _replace_recurse
from spinta.utils.data import take
from spinta.utils.schema import is_valid_sort_key
from spinta.types.datatype import Array, DataType, File, Object
from spinta.exceptions import NotFoundError, ItemDoesNotExist, UnavailableSubresource
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.constants import TableType
from spinta.backends.postgresql.helpers import get_column_name


@commands.getone.register()
async def getone(
    context: Context,
    request: Request,
    model: Model,
    backend: PostgreSQL,
    *,
    action: Action,
    params: UrlParams,
):
    commands.authorize(context, action, model)
    data = getone(context, model, backend, id_=params.pk)
    hidden_props = [prop.name for prop in model.properties.values() if prop.hidden]
    log_getone(context, data, select=params.select, hidden=hidden_props)
    data = commands.prepare_data_for_response(
        context,
        Action.GETONE,
        model,
        backend,
        data,
        select=params.select,
    )
    return render(context, request, model, params, data, action=action)


@commands.getone.register()
def getone(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    id_: str,
):
    connection = context.get('transaction').connection
    table = backend.get_table(model)
    try:
        result = backend.get(connection, table, table.c._id == id_)
    except NotFoundError:
        raise ItemDoesNotExist(model, id=id_)
    data = _flat_dicts_to_nested(dict(result))
    data['_type'] = model.model_type()
    return commands.cast_backend_to_python(context, model, backend, data)


def _flat_dicts_to_nested(value):
    res = {}
    for k, v in dict(value).items():
        names = k.split('.')
        vref = res
        for name in names[:-1]:
            if name not in vref:
                vref[name] = {}
            vref = vref[name]
        vref[names[-1]] = v
    return res


@commands.getone.register()
async def getone(
    context: Context,
    request: Request,
    prop: Property,
    dtype: Object,
    backend: PostgreSQL,
    *,
    action: Action,
    params: UrlParams,
):
    commands.authorize(context, action, prop)
    data = getone(context, prop, dtype, backend, id_=params.pk)
    log_getone(context, data)
    data = commands.prepare_data_for_response(
        context,
        Action.GETONE,
        prop.dtype,
        backend,
        data,
    )
    return render(context, request, prop, params, data, action=action)


@commands.getone.register()
async def getone(
    context: Context,
    request: Request,
    prop: Property,
    dtype: DataType,
    backend: PostgreSQL,
    *,
    action: Action,
    params: UrlParams,
):
    raise UnavailableSubresource(prop=prop.name, prop_type=prop.dtype.name)


@commands.getone.register()
def getone(
    context: Context,
    prop: Property,
    dtype: Object,
    backend: PostgreSQL,
    *,
    id_: str,
):
    table = backend.get_table(prop.model)
    connection = context.get('transaction').connection
    selectlist = [
        table.c._id,
        table.c._revision,
    ] + [
        table.c[name]
        for name in _iter_prop_names(prop.dtype)
    ]
    try:
        data = backend.get(connection, selectlist, table.c._id == id_)
    except NotFoundError:
        raise ItemDoesNotExist(prop.model, id=id_)

    result = {
        '_type': prop.model_type(),
        '_id': data[table.c._id],
        '_revision': data[table.c._revision],
    }

    data = _flat_dicts_to_nested(data)
    result[prop.name] = data[prop.name]
    return commands.cast_backend_to_python(context, prop, backend, result)


@dispatch((Model, Object))
def _iter_prop_names(dtype) -> Iterator[Property]:
    for prop in dtype.properties.values():
        yield from _iter_prop_names(prop.dtype)


@dispatch(DataType)
def _iter_prop_names(dtype) -> Iterator[Property]:  # noqa
    if not dtype.prop.name.startswith('_'):
        yield get_column_name(dtype.prop)


@dispatch(File)
def _iter_prop_names(dtype) -> Iterator[Property]:  # noqa
    yield dtype.prop.place + '._id'
    yield dtype.prop.place + '._content_type'


@commands.getone.register()
async def getone(
    context: Context,
    request: Request,
    prop: Property,
    dtype: File,
    backend: PostgreSQL,
    *,
    action: Action,
    params: UrlParams,
):
    commands.authorize(context, action, prop)
    data = getone(context, prop, dtype, backend, id_=params.pk)

    # Return file metadata
    if params.propref:
        log_getone(context, data)
        data = commands.prepare_data_for_response(
            context,
            Action.GETONE,
            prop.dtype,
            backend,
            data,
        )
        return render(context, request, prop, params, data, action=action)

    # Return file content
    else:
        value = take(prop.place, data)

        if not take('_blocks', value):
            raise ItemDoesNotExist(dtype, id=params.pk)

        filename = value['_id']

        connection = context.get('transaction').connection
        table = backend.get_table(prop, TableType.FILE)
        with DatabaseFile(
            connection,
            table,
            value['_size'],
            value['_blocks'],
            value['_bsize'],
            mode='r',
        ) as f:
            content = f.read()

        return Response(
            content,
            media_type=value['_content_type'],
            headers={
                'Revision': data['_revision'],
                'Content-Disposition': (
                    f'attachment; filename="{filename}"'
                    if filename else
                    'attachment'
                )
            },
        )


@commands.getone.register()
def getone(
    context: Context,
    prop: Property,
    dtype: File,
    backend: PostgreSQL,
    *,
    id_: str,
):
    table = backend.get_table(prop.model)
    connection = context.get('transaction').connection
    selectlist = [
        table.c._id,
        table.c._revision,
        table.c[prop.place + '._id'],
        table.c[prop.place + '._content_type'],
        table.c[prop.place + '._size'],
    ]

    if BackendFeatures.FILE_BLOCKS in prop.dtype.backend.features:
        selectlist += [
            table.c[prop.place + '._bsize'],
            table.c[prop.place + '._blocks'],
        ]

    try:
        data = backend.get(connection, selectlist, table.c._id == id_)
    except NotFoundError:
        raise ItemDoesNotExist(dtype, id=id_)

    result = {
        '_type': prop.model_type(),
        '_id': data[table.c._id],
        '_revision': data[table.c._revision],
    }

    data = _flat_dicts_to_nested(data)
    result[prop.name] = data[prop.name]
    return commands.cast_backend_to_python(context, prop, backend, result)


@commands.getfile.register()
def getfile(
    context: Context,
    prop: Property,
    dtype: File,
    backend: PostgreSQL,
    *,
    data: dict,
):
    if not data['_blocks']:
        return None

    if len(data['_blocks']) > 1:
        # TODO: Use propper UserError exception.
        raise Exception(
            "File content is to large to retrun it inline. Try accessing "
            "this file directly using subresources API."
        )

    connection = context.get('transaction').connection
    table = backend.get_table(prop, TableType.FILE)
    with DatabaseFile(
        connection,
        table,
        data['_size'],
        data['_blocks'],
        data['_bsize'],
        mode='r',
    ) as f:
        return f.read()


@commands.getall.register()
async def getall(
    context: Context,
    request: Request,
    model: Model,
    backend: PostgreSQL,
    *,
    action: Action,
    params: UrlParams,
):
    commands.authorize(context, action, model)
    rows = getall(
        context, model, backend,
        select=params.select,
        sort=params.sort,
        offset=params.offset,
        limit=params.limit,
        query=params.query,
        count=params.count,
    )
    hidden_props = [prop.name for prop in model.properties.values() if prop.hidden]
    rows = log_getall(context, rows, select=params.select, hidden=hidden_props)
    if not params.count:
        rows = (
            commands.prepare_data_for_response(
                context,
                action,
                model,
                backend,
                row,
                select=params.select,
            )
            for row in rows
        )
    return render(context, request, model, params, rows, action=action)


@commands.getall.register()
def getall(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    action: Action = Action.GETALL,
    select: List[str] = None,
    sort: Dict[str, dict] = None,
    offset: int = None,
    limit: int = None,
    query: List[Dict[str, str]] = None,
    count: bool = False,
):
    connection = context.get('transaction').connection

    if count:
        table = backend.get_table(model)
        qry = sa.select([sa.func.count()]).select_from(table)
        result = connection.execute(qry)
        yield {'count': result.scalar()}
        return

    qb = QueryBuilder(context, model, backend)
    qry = qb.build(select, sort, offset, limit, query)

    for row in connection.execute(qry):
        row = _flat_dicts_to_nested(dict(row))
        row = {
            '_type': model.model_type(),
            **row,
        }
        yield commands.cast_backend_to_python(context, model, backend, row)


class QueryBuilder:
    compops = (
        'eq',
        'ge',
        'gt',
        'le',
        'lt',
        'ne',
        'contains',
        'startswith',
    )

    def __init__(
        self,
        context: Context,
        model: Model,
        backend: PostgreSQL,
    ):
        self.context = context
        self.model = model
        self.backend = backend
        self.select = []
        self.where = []
        self.joins = backend.get_table(model)

    def build(
        self,
        select: List[str] = None,
        sort: Dict[str, dict] = None,
        offset: int = None,
        limit: int = None,
        query: Optional[List[dict]] = None,
    ) -> sa.sql.Select:
        # TODO: Select list must be taken from params.select.
        qry = sa.select([self.backend.get_table(self.model)])

        if query:
            qry = qry.where(self.op_and(*query))

        if sort:
            qry = self.sort(qry, sort)

        qry = _getall_offset(qry, offset)
        qry = _getall_limit(qry, limit)
        qry = qry.select_from(self.joins)
        return qry

    def _get_method(self, name):
        method = getattr(self, f'op_{name}', None)
        if method is None:
            raise exceptions.UnknownOperator(self.model, operator=name)
        return method

    def resolve_recurse(self, arg):
        name = arg['name']
        if name in self.compops:
            return _replace_recurse(self.model, arg, 0)
        if name == 'any':
            return _replace_recurse(self.model, arg, 1)
        return arg

    def resolve(self, args: Optional[List[dict]]) -> None:
        for arg in (args or []):
            arg = self.resolve_recurse(arg)
            name = arg['name']
            opargs = arg.get('args', ())
            method = self._get_method(name)
            if name in self.compops:
                yield self.comparison(name, method, *opargs)
            else:
                yield method(*opargs)

    def resolve_property(self, key: Union[str, tuple], sort: bool = False) -> Property:
        if isinstance(key, tuple):
            key = '.'.join(key)

        if sort:
            if not is_valid_sort_key(key, self.model):
                raise exceptions.FieldNotInResource(self.model, property=key)
        elif key not in self.model.flatprops:
            raise exceptions.FieldNotInResource(self.model, property=key)

        prop = self.model.flatprops[key]
        if isinstance(prop.dtype, Array):
            return prop.dtype.items
        else:
            return prop

    def resolve_value(self, op: str, prop: Property, value: Union[str, dict]) -> object:
        return commands.load_search_params(self.context, prop.dtype, self.backend, {
            'name': op,
            'args': [prop.place, value]
        })

    def resolve_lower_call(self, key):
        if isinstance(key, dict) and key['name'] == 'lower':
            return key['args'][0], True
        else:
            return key, False

    def comparison(self, op, method, key, value):
        key, lower = self.resolve_lower_call(key)
        prop = self.resolve_property(key)
        value = self.resolve_value(op, prop, value)
        field = self.get_sql_field(prop, lower)
        value = self.get_sql_value(prop, value)
        cond = method(prop, field, value)
        return self.compare(op, prop, cond)

    def compare(self, op, prop, cond):
        if prop.list is not None and op != 'ne':
            main_table = self.backend.get_table(self.model)
            list_table = self.backend.get_table(prop.list, TableType.LIST)
            subqry = (
                sa.select(
                    [list_table.c._rid],
                    distinct=list_table.c._rid,
                ).
                where(cond).
                alias()
            )
            self.joins = self.joins.outerjoin(
                subqry,
                main_table.c._id == subqry.c._rid,
            )
            return subqry.c._rid.isnot(None)
        else:
            return cond

    def get_sql_field(self, prop: Property, lower: bool = False):
        if prop.list is not None:
            list_table = self.backend.get_table(prop.list, TableType.LIST)
            field = list_table.c[get_column_name(prop)]
        else:
            main_table = self.backend.get_table(self.model)
            field = main_table.c[prop.place]
        if lower:
            field = sa.func.lower(field)
        return field

    def get_sql_value(self, prop: Property, value: object):
        return value

    def op_group(self, *args: List[dict]):
        args = list(self.resolve(args))
        assert len(args) == 1, "Group with multiple args are not supported here."
        return args[0]

    def op_and(self, *args: List[dict]):
        return sa.and_(*self.resolve(args))

    def op_or(self, *args: List[dict]):
        return sa.or_(*self.resolve(args))

    def op_eq(self, prop, field, value):
        return field == value

    def op_ge(self, prop, field, value):
        return field >= value

    def op_gt(self, prop, field, value):
        return field > value

    def op_le(self, prop, field, value):
        return field <= value

    def op_lt(self, prop, field, value):
        return field < value

    def op_ne(self, prop, field, value):
        """Not equal operator is quite complicated thing and need explaining.

        If property is not defined within a list, just do `!=` comparison and be
        done with it.

        If property is in a list:

        - First check if there is at least one list item where field is not None
          (existance check).

        - Then check if there is no list items where field equals to given
          value.
        """

        if prop.list is None:
            return field != value

        main_table = self.backend.get_table(self.model)
        list_table = self.backend.get_table(prop.list, TableType.LIST)

        # Check if at liest one value for field is defined
        subqry1 = (
            sa.select(
                [list_table.c._rid],
                distinct=list_table.c._rid,
            ).
            where(field != None).  # noqa
            alias()
        )
        self.joins = self.joins.outerjoin(
            subqry1,
            main_table.c._id == subqry1.c._rid,
        )

        # Check if given value exists
        subqry2 = (
            sa.select(
                [list_table.c._rid],
                distinct=list_table.c._rid,
            ).
            where(field == value).
            alias()
        )
        self.joins = self.joins.outerjoin(
            subqry2,
            main_table.c._id == subqry2.c._rid,
        )

        # If field exists and given value does not, then field is not equal to
        # value.
        return sa.and_(
            subqry1.c._rid != None,  # noqa
            subqry2.c._rid == None,
        )

    def op_contains(self, prop, field, value):
        if isinstance(field.type, UUID):
            return field.cast(sa.String).contains(value)
        return field.contains(value)

    def op_startswith(self, prop, field, value):
        if isinstance(field.type, UUID):
            return field.cast(sa.String).startswith(value)
        return field.startswith(value)

    def op_any(self, op: str, key: str, *value: Tuple[Union[str, int, float]]):
        if op in ('contains', 'startswith'):
            return self.op_or(*(
                {
                    'name': op,
                    'args': [key, v],
                }
                for v in value
            ))

        method = self._get_method(op)
        key, lower = self.resolve_lower_call(key)
        prop = self.resolve_property(key)
        field = self.get_sql_field(prop, lower)
        value = [
            self.get_sql_value(prop, self.resolve_value(op, prop, v))
            for v in value
        ]
        value = sa.any_(value)
        cond = method(prop, field, value)
        return self.compare(op, prop, cond)

    def sort(
        self,
        qry: sa.sql.Select,
        sort: List[Tuple[str, str]],
    ) -> sa.sql.Select:
        direction = {
            'positive': lambda c: c.asc(),
            'negative': lambda c: c.desc(),
        }
        fields = []
        for key in sort:
            # Optional sort direction: sort(+key) or sort(key)
            # XXX: Probably move this to spinta/urlparams.py.
            if isinstance(key, dict) and key['name'] in direction:
                d = direction[key['name']]
                key = key['args'][0]
            else:
                d = direction['positive']

            key, lower = self.resolve_lower_call(key)
            prop = self.resolve_property(key, sort=True)
            field = self.get_sql_field(prop, lower)

            main_table = self.backend.get_table(self.model)
            if prop.list is not None:
                list_table = self.backend.get_table(prop.list, TableType.LIST)
                subqry = (
                    sa.select(
                        [list_table.c._rid, field.label('value')],
                        distinct=list_table.c._rid,
                    ).alias()
                )
                self.joins = self.joins.outerjoin(
                    subqry,
                    main_table.c._id == subqry.c._rid,
                )
                field = subqry.c.value
            else:
                field = main_table.c[prop.place]

            if lower:
                field = sa.func.lower(field)

            field = d(field)
            fields.append(field)

        return qry.order_by(*fields)


def _getall_offset(qry: sa.sql.Select, offset: Optional[int]) -> sa.sql.Select:
    if offset:
        return qry.offset(offset)
    else:
        return qry


def _getall_limit(qry: sa.sql.Select, limit: Optional[int]) -> sa.sql.Select:
    if limit:
        return qry.limit(limit)
    else:
        return qry
