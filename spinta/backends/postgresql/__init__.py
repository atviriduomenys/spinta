from typing import AsyncIterator, Optional, List, Union, Tuple, Dict, Iterator

import contextlib
import datetime
import enum
import hashlib
import itertools
import typing
import types

import sqlalchemy as sa
import sqlalchemy.exc
from sqlalchemy.dialects.postgresql import JSONB, BIGINT, UUID
from sqlalchemy.engine.result import RowProxy
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import FunctionElement
from sqlalchemy.sql.type_api import TypeEngine
from starlette.requests import Request
from multipledispatch import dispatch

from spinta import commands
from spinta import exceptions
from spinta.backends import Backend
from spinta.commands import wait, load, prepare, migrate, getone, getall, wipe, authorize
from spinta.components import Context, Manifest, Model, Property, Action, UrlParams, DataItem
from spinta.config import RawConfig
from spinta.hacks.recurse import _replace_recurse
from spinta.renderer import render
from spinta.utils.schema import NA, strip_metadata
from spinta.types.datatype import (
    Array,
    DataType,
    Date,
    DateTime,
    File,
    Object,
    PrimaryKey,
    Ref,
)

from spinta.exceptions import (
    MultipleRowsFound,
    NotFoundError,
    ItemDoesNotExist,
    UniqueConstraint,
    UnavailableSubresource,
)

# Maximum length for PostgreSQL identifiers (e.g. table names, column names,
# function names).
# https://github.com/postgres/postgres/blob/master/src/include/pg_config_manual.h
NAMEDATALEN = 63

UNSUPPORTED_TYPES = [
    'backref',
    'generic',
    'rql',
]


class TableType(enum.Enum):
    MAIN = ''
    LIST = '/:list'
    CHANGELOG = '/:changelog'
    CACHE = '/:cache'


class PostgreSQL(Backend):
    metadata = {
        'name': 'postgresql',
        'properties': {
            'dsn': {'type': 'string', 'required': True},
        },
    }

    engine = None
    schema = None
    tables = None

    @contextlib.contextmanager
    def transaction(self, write=False):
        with self.engine.begin() as connection:
            if write:
                table = self.tables['internal']['transaction']
                result = connection.execute(
                    table.main.insert().values(
                        datetime=utcnow(),
                        client_type='',
                        client_id='',
                        errors=0,
                    )
                )
                transaction_id = result.inserted_primary_key[0]
                yield WriteTransaction(connection, transaction_id)
            else:
                yield ReadTransaction(connection)

    def get(self, connection, columns, condition, default=NA):
        scalar = isinstance(columns, sa.Column)
        columns = columns if isinstance(columns, list) else [columns]

        result = connection.execute(
            sa.select(columns).where(condition)
        )
        result = list(itertools.islice(result, 2))

        if len(result) == 1:
            if scalar:
                return result[0][columns[0]]
            else:
                return result[0]

        elif len(result) == 0:
            if default is NA:
                raise NotFoundError()
            else:
                return default
        else:
            raise MultipleRowsFound()


class ReadTransaction:

    def __init__(self, connection):
        self.connection = connection


class WriteTransaction(ReadTransaction):

    def __init__(self, connection, id):
        super().__init__(connection)
        self.id = id
        self.errors = 0


@wait.register()
def wait(context: Context, backend: PostgreSQL, config: RawConfig, *, fail: bool = False):
    dsn = config.get('backends', backend.name, 'dsn', required=True)
    engine = sa.create_engine(dsn, connect_args={'connect_timeout': 0})
    try:
        conn = engine.connect()
    except sqlalchemy.exc.OperationalError:
        if fail:
            raise
        else:
            return False
    else:
        conn.close()
        engine.dispose()
        return True


@load.register()
def load(context: Context, backend: PostgreSQL, config: RawConfig):
    backend.dsn = config.get('backends', backend.name, 'dsn', required=True)
    backend.engine = sa.create_engine(backend.dsn, echo=False)
    backend.schema = sa.MetaData(backend.engine)
    backend.tables = {}


@prepare.register()
def prepare(context: Context, backend: PostgreSQL, manifest: Manifest):
    if manifest.name not in backend.tables:
        backend.tables[manifest.name] = {}

    # Prepare backend for models.
    for model in manifest.objects['model'].values():
        if model.backend.name == backend.name:
            prepare(context, backend, model)

    # Prepare backend for datasets.
    for dataset in manifest.objects.get('dataset', {}).values():
        for resource in dataset.resources.values():
            for model in resource.models():
                if model.backend.name == backend.name:
                    prepare(context, backend, model)


@prepare.register()
def prepare(context: Context, backend: PostgreSQL, model: Model):
    columns = []
    for prop in model.properties.values():
        if prop.name.startswith('_') and prop.name not in ('_id', '_revision'):
            continue
        column = prepare(context, backend, prop)
        if isinstance(column, list):
            columns.extend(column)
        elif column is not None:
            columns.append(column)

    # Create main table.
    main_table_name = get_table_name(model)
    main_table = sa.Table(
        main_table_name, backend.schema,
        sa.Column('_transaction', sa.Integer, sa.ForeignKey('transaction._id')),
        sa.Column('_created', sa.DateTime),
        sa.Column('_updated', sa.DateTime),
        *columns,
    )

    # Create tables for nested lists, these tables are used for searches.
    lists_tables = {}
    for prop in _iter_lists(model):
        columns = prepare(context, backend, prop.dtype.items)
        assert columns is not None
        if not isinstance(columns, list):
            columns = [columns]
        lists_table_name = get_table_name(prop, TableType.LIST)
        lists_table = sa.Table(
            lists_table_name, backend.schema,
            sa.Column('_txn', sa.Integer, sa.ForeignKey('transaction._id')),
            sa.Column('_id', main_table.c._id.type, sa.ForeignKey(
                f'{main_table_name}._id', ondelete='CASCADE',
            )),
            *columns,
        )
        lists_tables[prop.place] = lists_table

    # Create changes table.
    changes_table_name = get_table_name(model, TableType.CHANGELOG)
    # XXX: not sure if I should pass main_table.c.id.type.__class__() or a
    #      shorter form.
    changes_table = get_changes_table(backend, changes_table_name, main_table.c._id.type)

    backend.tables[model.manifest.name][model.name] = ModelTables(main_table, lists_tables, changes_table)


@prepare.register()
def prepare(context: Context, backend: PostgreSQL, dtype: DataType):
    if dtype.prop.backend.name != backend.name:
        # If property backend differs from model backend, then no columns should
        # be added to the table. If some property type require adding columns
        # even for a property with different backend, then this type must
        # implement prepare command and do custom logic there.
        return

    prop = dtype.prop
    name = _get_column_name(prop)

    if dtype.name == 'string':
        return sa.Column(name, sa.Text)
    elif dtype.name == 'date':
        return sa.Column(name, sa.Date)
    elif dtype.name == 'datetime':
        return sa.Column(name, sa.DateTime)
    elif dtype.name == 'integer':
        return sa.Column(name, sa.Integer)
    elif dtype.name == 'number':
        return sa.Column(name, sa.Float)
    elif dtype.name == 'boolean':
        return sa.Column(name, sa.Boolean)
    elif dtype.name in ('spatial', 'image'):
        # TODO: these property types currently are not implemented
        return sa.Column(name, sa.Text)
    elif dtype.name in UNSUPPORTED_TYPES:
        return
    else:
        raise Exception(
            f"Unknown property type {dtype.name!r} of {prop.place}."
        )


def _get_column_name(prop: Property):
    if prop.list:
        if prop.place == prop.list.place:
            return prop.list.name
        else:
            return prop.place[len(prop.list.place) + 1:]
    else:
        return prop.place


@prepare.register()
def prepare(context: Context, backend: PostgreSQL, dtype: PrimaryKey):
    if dtype.prop.manifest.name == 'internal':
        return sa.Column('_id', BIGINT, primary_key=True)
    else:
        return sa.Column('_id', UUID(), primary_key=True)


@prepare.register()
def prepare(context: Context, backend: PostgreSQL, dtype: Ref):
    # TODO: rename dtype.object to dtype.model
    ref_model = commands.get_referenced_model(context, dtype.prop, dtype.object)
    if ref_model.manifest.name == 'internal':
        column_type = sa.Integer()
    else:
        column_type = UUID()
    return get_pg_foreign_key(
        dtype.prop,
        table_name=get_table_name(ref_model),
        model_name=dtype.prop.model.name,
        column_type=column_type,
    )


def get_pg_foreign_key(
    prop: Property,
    *,
    table_name: str,
    model_name: str,
    column_type: TypeEngine,
) -> List[Union[sa.Column, sa.Constraint]]:
    column_name = _get_column_name(prop) + '._id'
    return [
        sa.Column(column_name, column_type),
        sa.ForeignKeyConstraint(
            [column_name], [f'{table_name}._id'],
            name=_get_pg_name(f'fk_{model_name}_{column_name}'),
        )
    ]


@prepare.register()
def prepare(context: Context, backend: PostgreSQL, dtype: File):
    name = _get_column_name(dtype.prop)
    if dtype.prop.backend.name == backend.name:
        return [
            sa.Column(f'{name}._content_type', sa.String),
            sa.Column(name, sa.LargeBinary),
        ]
    else:
        return [
            sa.Column(f'{name}._id', sa.String),
            sa.Column(f'{name}._content_type', sa.String),
        ]


@prepare.register()
def prepare(context: Context, backend: PostgreSQL, dtype: Array):
    prop = dtype.prop
    if prop.list is None:
        return sa.Column(prop.place, JSONB)


@prepare.register()
def prepare(context: Context, backend: PostgreSQL, dtype: Object):
    columns = []
    for prop in dtype.properties.values():
        if prop.name.startswith('_') and prop.name not in ('_revision',):
            continue
        column = prepare(context, backend, prop)
        if isinstance(column, list):
            columns.extend(column)
        elif column is not None:
            columns.append(column)
    return columns


def _get_pg_name(name):
    if len(name) > NAMEDATALEN:
        name_hash = hashlib.sha256(name.encode()).hexdigest()
        return name[:NAMEDATALEN - 7] + '_' + name_hash[-6:]
    else:
        return name


class ModelTables(typing.NamedTuple):
    main: sa.Table = None
    lists: sa.Table = None
    changes: sa.Table = None
    cache: sa.Table = None


@migrate.register()
def migrate(context: Context, backend: PostgreSQL):
    # XXX: I found, that this some times leaks connection, you can check that by
    #      comparing `backend.engine.pool.checkedin()` before and after this
    #      line.
    backend.schema.create_all(checkfirst=True)


@commands.check_unique_constraint.register()
def check_unique_constraint(
    context: Context,
    data: DataItem,
    dtype: DataType,
    prop: Property,
    backend: PostgreSQL,
    value: object,
):
    table = backend.tables[prop.manifest.name][prop.model.name].main

    if data.action in (Action.UPDATE, Action.PATCH):
        if prop.name == '_id' and value == data.saved['_id']:
            return
        condition = sa.and_(
            table.c[prop.name] == value,
            table.c._id != data.saved['_id'],
        )
    else:
        condition = table.c[prop.name] == value
    not_found = object()
    connection = context.get('transaction').connection
    result = backend.get(connection, table.c[prop.name], condition, default=not_found)
    if result is not not_found:
        raise UniqueConstraint(prop)


def _update_lists_table(
    context: Context,
    model: Model,
    tables: Dict[str, sa.Table],
    action: Action,
    pk: str,
    patch: dict,
) -> None:
    if len(tables) == 0:
        return

    transaction = context.get('transaction')
    connection = transaction.connection
    rows = _get_lists_data(model, patch)
    sort_key = lambda x: x[0].place  # noqa
    rows = sorted(rows, key=sort_key)
    for place, rows in itertools.groupby(rows, key=sort_key):
        table = tables[place]
        if action != Action.INSERT:
            connection.execute(table.delete().where(table.c._id == pk))
        rows = [
            {
                'transaction': transaction.id,
                '_id': pk,
                **{
                    _get_list_column_name(place, k): v
                    for k, v in row.items()
                }
            }
            for prop, row in rows
        ]
        connection.execute(table.insert(), rows)


def _get_list_column_name(place, name):
    if place == name:
        return place.split('.')[-1]
    else:
        return name[len(place) + 1:]


def _has_lists(dtype: Union[Model, DataType]):
    if isinstance(dtype, (Model, Object)):
        for prop in dtype.properties.values():
            if _has_lists(prop.dtype):
                return True
    elif isinstance(dtype, Array):
        return True
    else:
        return False


def _iter_lists(dtype: Union[Model, DataType]):
    if isinstance(dtype, (Model, Object)):
        for prop in dtype.properties.values():
            yield from _iter_lists(prop.dtype)
    elif isinstance(dtype, Array):
        yield dtype.prop
        yield from _iter_lists(dtype.items.dtype)


def _get_lists_data(
    dtype: Union[Model, DataType],
    value: object,
) -> List[dict]:
    data, lists = _separate_lists_and_data(dtype, value)
    if isinstance(dtype, DataType) and data is not NA:
        yield dtype.prop, data
    for prop, vals in lists:
        for v in vals:
            yield from _get_lists_data(prop.dtype, v)


def _separate_lists_and_data(
    dtype: Union[Model, DataType],
    value: object,
) -> Tuple[dict, List[Tuple[Property, list]]]:
    if isinstance(dtype, (Model, Object)):
        data = {}
        lists = []
        for k, v in (value or {}).items():
            prop = dtype.properties[k]
            v, more = _separate_lists_and_data(prop.dtype, v)
            if v is not NA:
                data.update(v)
            if more:
                lists += more
        return data or NA, lists
    elif isinstance(dtype, Array):
        if value:
            return NA, [(dtype.items, value)]
        else:
            return NA, []
    else:
        return {dtype.prop.place: value}, []


@commands.insert.register()
async def insert(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    dstream: AsyncIterator[DataItem],
    stop_on_error: bool = True,
):
    transaction = context.get('transaction')
    connection = transaction.connection
    table = backend.tables[model.manifest.name][model.name]
    async for data in dstream:
        # TODO: Refactor this to insert batches with single query.
        qry = table.main.insert().values(
            _id=data.patch['_id'],
            _revision=data.patch['_revision'],
            _transaction=transaction.id,
            _created=utcnow(),
        )

        patch = commands.build_full_data_patch_for_nested_attrs(
            context,
            model,
            patch=strip_metadata(data.patch),
            saved=None,
        )

        connection.execute(qry, patch)

        # Update lists table
        _update_lists_table(
            context,
            model,
            table.lists,
            Action.INSERT,
            data.patch['_id'],
            data.patch,
        )

        yield data


@commands.update.register()
async def update(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    dstream: dict,
    stop_on_error: bool = True,
):
    transaction = context.get('transaction')
    connection = transaction.connection
    table = backend.tables[model.manifest.name][model.name]

    async for data in dstream:
        if not data.patch:
            yield data
            continue

        id_ = data.saved['_id']

        # Support patching nested properties.
        # Build a full_patch dict, where data.patch is merged with data.saved
        # for nested properties.
        patch = commands.build_full_data_patch_for_nested_attrs(
            context,
            model,
            patch=strip_metadata(data.patch),
            saved=strip_metadata(data.saved),
        )

        patch['_revision'] = data.patch['_revision']
        if '_id' in data.patch:
            patch['_id'] = data.patch['_id']

        result = connection.execute(
            table.main.update().
            where(table.main.c._id == id_).
            where(table.main.c._revision == data.saved['_revision']).
            values(patch)
        )

        if result.rowcount == 0:
            raise Exception(f"Update failed, {model} with {id_} not found.")
        elif result.rowcount > 1:
            raise Exception(f"Update failed, {model} with {id_} has found and update {result.rowcount} rows.")

        # Update lists table
        patch = {
            k: v for k, v in data.patch.items() if not k.startswith('_')
        }
        _update_lists_table(
            context,
            model,
            table.lists,
            data.action,
            id_,
            patch,
        )

        yield data


@commands.delete.register()
async def delete(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    dstream: AsyncIterator[DataItem],
    stop_on_error: bool = True,
):
    transaction = context.get('transaction')
    connection = transaction.connection
    table = backend.tables[model.manifest.name][model.name]
    async for data in dstream:
        connection.execute(
            table.main.delete().
            where(table.main.c._id == data.saved['_id'])
        )

        yield data


@getone.register()
async def getone(
    context: Context,
    request: Request,
    model: Model,
    backend: PostgreSQL,
    *,
    action: Action,
    params: UrlParams,
):
    authorize(context, action, model)
    data = getone(context, model, backend, id_=params.pk)
    # TODO: All meta properties eventually should have `_` prefix.
    data['id'] = data['_id']
    data = prepare(context, Action.GETONE, model, backend, data, select=params.select)
    return render(context, request, model, params, data, action=action)


@getone.register()
def getone(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    id_: str,
):
    connection = context.get('transaction').connection
    table = backend.tables[model.manifest.name][model.name].main
    try:
        result = backend.get(connection, table, table.c._id == id_)
    except NotFoundError:
        raise ItemDoesNotExist(model, id=id_)
    return _flat_dicts_to_nested(dict(result))


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


@getone.register()
async def getone(
    context: Context,
    request: Request,
    prop: Property,
    dtype: (Object, File),
    backend: PostgreSQL,
    *,
    action: Action,
    params: UrlParams,
):
    authorize(context, action, prop)
    data = getone(context, prop, dtype, backend, id_=params.pk)
    pdata = data.pop(prop.name)
    data = {
        **data,
        **pdata,
    }
    data = prepare(context, Action.GETONE, prop.dtype, backend, data)
    return render(context, request, prop, params, data, action=action)


@getone.register()
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


@getone.register()
def getone(
    context: Context,
    prop: Property,
    dtype: Object,
    backend: PostgreSQL,
    *,
    id_: str,
):
    table = backend.tables[prop.manifest.name][prop.model.name].main
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
        '_id': data[table.c._id],
        '_revision': data[table.c._revision],
        '_type': prop.model.model_type(),
    }

    data = _flat_dicts_to_nested(data)
    result[prop.name] = data[prop.name]
    return result


@dispatch((Model, Object))
def _iter_prop_names(dtype) -> Iterator[Property]:
    for prop in dtype.properties.values():
        yield from _iter_prop_names(prop.dtype)


@dispatch(DataType)
def _iter_prop_names(dtype) -> Iterator[Property]:  # noqa
    if not dtype.prop.name.startswith('_'):
        yield _get_column_name(dtype.prop)


@dispatch(File)
def _iter_prop_names(dtype) -> Iterator[Property]:  # noqa
    yield dtype.prop.place + '._id'
    yield dtype.prop.place + '._content_type'


@getone.register()
def getone(
    context: Context,
    prop: Property,
    dtype: File,
    backend: PostgreSQL,
    *,
    id_: str,
):
    table = backend.tables[prop.manifest.name][prop.model.name].main
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
        '_id': data[table.c._id],
        '_revision': data[table.c._revision],
        '_type': prop.model_type(),
    }

    data = _flat_dicts_to_nested(data)
    result[prop.name] = data[prop.name]
    return result


@getall.register()
async def getall(
    context: Context,
    request: Request,
    model: Model,
    backend: PostgreSQL,
    *,
    action: Action,
    params: UrlParams,
):
    authorize(context, action, model)
    result = (
        prepare(context, action, model, backend, row, select=params.select)
        for row in getall(
            context, model, backend,
            select=params.select,
            sort=params.sort,
            offset=params.offset,
            limit=params.limit,
            query=params.query,
            # TODO: Add count support.
        )
    )
    return render(context, request, model, params, result, action=action)


@getall.register()
def getall(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    action: Action = Action.GETALL,
    select: typing.List[str] = None,
    sort: typing.Dict[str, dict] = None,
    offset: int = None,
    limit: int = None,
    query: typing.List[typing.Dict[str, str]] = None,
    count: bool = False,
):
    connection = context.get('transaction').connection
    table = backend.tables[model.manifest.name][model.name]

    qb = QueryBuilder(context, model, backend, table)
    qry = qb.build(select, sort, offset, limit, query)

    for row in connection.execute(qry):
        row = _flat_dicts_to_nested(dict(row))
        yield {
            '_type': model.model_type(),
            **row,
        }


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
        table: ModelTables,
    ):
        self.context = context
        self.model = model
        self.backend = backend
        self.table = table
        self.select = []
        self.where = []
        self.joins = table.main

    def build(
        self,
        select: typing.List[str] = None,
        sort: typing.Dict[str, dict] = None,
        offset: int = None,
        limit: int = None,
        query: Optional[List[dict]] = None,
    ) -> sa.sql.Select:
        # TODO: Select list must be taken from params.select.
        qry = sa.select([self.table.main])

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

    def resolve_property(self, key: Union[str, tuple]) -> Property:
        if isinstance(key, tuple):
            key = '.'.join(key)
        if key not in self.model.flatprops:
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
            list_table = self.table.lists[prop.list.place]
            subqry = (
                sa.select(
                    [list_table.c._id],
                    distinct=list_table.c._id,
                ).
                where(cond).
                alias()
            )
            self.joins = self.joins.outerjoin(
                subqry,
                self.table.main.c._id == subqry.c._id,
            )
            return subqry.c._id.isnot(None)
        else:
            return cond

    def get_sql_field(self, prop: Property, lower: bool = False):
        if prop.list is not None:
            list_table = self.table.lists[prop.list.place]
            field = list_table.c[_get_column_name(prop)]
        else:
            field = self.table.main.c[prop.place]
        if lower:
            field = sa.func.lower(field)
        return field

    def get_sql_value(self, prop: Property, value: object):
        return value

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

        list_table = self.table.lists[prop.list.place]

        # Check if at liest one value for field is defined
        subqry1 = (
            sa.select(
                [list_table.c._id],
                distinct=list_table.c._id,
            ).
            where(field != None).  # noqa
            alias()
        )
        self.joins = self.joins.outerjoin(
            subqry1,
            self.table.main.c._id == subqry1.c._id,
        )

        # Check if given value exists
        subqry2 = (
            sa.select(
                [list_table.c._id],
                distinct=list_table.c._id,
            ).
            where(field == value).
            alias()
        )
        self.joins = self.joins.outerjoin(
            subqry2,
            self.table.main.c._id == subqry2.c._id,
        )

        # If field exists and given value does not, then field is not equal to
        # value.
        return sa.and_(
            subqry1.c._id != None,  # noqa
            subqry2.c._id == None,
        )

    def op_contains(self, prop, field, value):
        return field.contains(value)

    def op_startswith(self, prop, field, value):
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
        sort: typing.List[typing.Tuple[str, str]],
    ) -> sa.sql.Select:
        direction = {
            '+': lambda c: c.asc(),
            '-': lambda c: c.desc(),
        }
        fields = []
        for key in sort:
            # Optional sort direction: sort(+key) or sort(key)
            # XXX: Probably move this to spinta/urlparams.py.
            if len(key) == 1:
                d, key = ('+',) + key
            else:
                d, key = key

            key, lower = self.resolve_lower_call(key)
            prop = self.resolve_property(key)
            field = self.get_sql_field(prop, lower)

            if prop.list is not None:
                list_table = self.table.lists[prop.list.place]
                subqry = (
                    sa.select(
                        [list_table.c._id, field.label('value')],
                        distinct=list_table.c._id,
                    ).alias()
                )
                self.joins = self.joins.outerjoin(
                    subqry,
                    self.table.main.c._id == subqry.c._id,
                )
                field = subqry.c.value
            else:
                field = self.table.main.c[prop.place]

            if lower:
                field = sa.func.lower(field)

            field = direction[d](field)
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


@commands.changes.register()
async def changes(
    context: Context,
    request: Request,
    model: Model,
    backend: PostgreSQL,
    *,
    action: Action,
    params: UrlParams,
):
    authorize(context, action, model)
    data = changes(context, model, backend, id_=params.pk, limit=params.limit, offset=params.offset)
    data = (
        {
            **row,
            '_created': row['_created'].isoformat(),
        }
        for row in data
    )
    return render(context, request, model, params, data, action=action)


@changes.register()
def changes(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    id_: str = None,
    limit: int = 100,
    offset: int = -10,
):
    connection = context.get('transaction').connection
    table = backend.tables[model.manifest.name][model.name].changes

    qry = sa.select([table]).order_by(table.c.change)
    qry = _changes_id(table, qry, id_)
    qry = _changes_offset(table, qry, offset)
    qry = _changes_limit(qry, limit)

    result = connection.execute(qry)
    for row in result:
        yield {
            '_change': row[table.c.change],
            '_revision': row[table.c.revision],
            '_transaction': row[table.c.transaction],
            '_id': row[table.c.id],
            '_created': row[table.c.datetime],
            '_op': row[table.c.action],
            **dict(row[table.c.data]),
        }


def _changes_id(table, qry, id_):
    if id_:
        return qry.where(table.c.id == id_)
    else:
        return qry


def _changes_offset(table, qry, offset):
    if offset:
        if offset > 0:
            offset = offset
        else:
            offset = (
                qry.with_only_columns([
                    sa.func.max(table.c.change) - abs(offset),
                ]).
                order_by(None).alias()
            )
        return qry.where(table.c.change > offset)
    else:
        return qry


def _changes_limit(qry, limit):
    if limit:
        return qry.limit(limit)
    else:
        return qry


@wipe.register()
def wipe(context: Context, model: Model, backend: PostgreSQL):
    if (
        model.manifest.name not in backend.tables or
        model.name not in backend.tables[model.manifest.name]
    ):
        # Model backend might not be prepared, this is especially true for
        # tests. So if backend is not yet prepared, just skipt this model.
        return

    table = backend.tables[model.manifest.name][model.name]
    connection = context.get('transaction').connection

    for prop in _iter_lists(model):
        connection.execute(table.lists[prop.place].delete())
    connection.execute(table.changes.delete())
    connection.execute(table.main.delete())


class utcnow(FunctionElement):
    type = sa.DateTime()


@compiles(utcnow, 'postgresql')
def pg_utcnow(element, compiler, **kw):
    return "TIMEZONE('utc', CURRENT_TIMESTAMP)"


def get_table_name(
    node: Union[Model, Property],
    ttype: TableType = TableType.MAIN,
):
    if ttype == TableType.LIST:
        name = node.model.model_type() + ttype.value + '/' + node.place
    else:
        name = node.model_type() + ttype.value
    if len(name) > NAMEDATALEN:
        hs = 8
        h = hashlib.sha1(name.encode()).hexdigest()[:hs]
        i = int(NAMEDATALEN / 100 * 60)
        j = NAMEDATALEN - i - hs - 2
        name = name[:i] + '_' + h + '_' + name[-j:]
    return name


def get_changes_table(backend, table_name, id_type):
    table = sa.Table(
        table_name, backend.schema,
        # XXX: This will not work with multi master setup. Consider changing it
        #      to UUID or something like that.
        #
        #      `change` should be monotonically incrementing, in order to
        #      have that, we could always create new `change_id`, by querying,
        #      previous `change_id` and increment it by one. This will create
        #      duplicates, but we simply know, that these changes happened at at
        #      the same time. So that's probably OK.
        sa.Column('change', BIGINT, primary_key=True),
        sa.Column('revision', sa.String(40)),
        sa.Column('transaction', sa.Integer, sa.ForeignKey('transaction._id')),
        sa.Column('id', id_type),  # reference to main table
        sa.Column('datetime', sa.DateTime),
        sa.Column('action', sa.String(8)),  # insert, update, delete
        sa.Column('data', JSONB),
    )
    return table


@prepare.register()
def prepare(
    context: Context,
    action: Action,
    model: Model,
    backend: PostgreSQL,
    value: RowProxy,
    *,
    select: typing.List[str] = None,
) -> dict:
    return prepare(context, action, model, backend, dict(value), select=select)


@prepare.register()
def prepare(
    context: Context,
    dtype: DateTime,
    backend: PostgreSQL,
    value: datetime.datetime,
) -> object:
    # convert datetime object to isoformat string if it belongs
    # to a nested property
    if dtype.prop.parent is dtype.prop.model:
        return value
    else:
        return value.isoformat()


@prepare.register()
def prepare(context: Context, dtype: Date, backend: PostgreSQL, value: datetime.date) -> object:
    # convert date object to isoformat string if it belongs
    # to a nested property
    if dtype.prop.parent is dtype.prop.model:
        return value
    else:
        return value.isoformat()


@commands.unload_backend.register()
def unload_backend(context: Context, backend: PostgreSQL):
    # Make sure all connections are released, since next test will create
    # another connection pool and connection pool is not reused between
    # tests. Maybe it would be a good idea to reuse same connection between
    # all tests?
    backend.engine.dispose()


def _fix_data_for_json(data):
    # XXX: a temporary workaround
    #
    #      Changelog data are stored as JSON and data must be JSON serializable.
    #      Probably there should be a command, that would make data JSON
    #      serializable.
    _data = {}
    for k, v in data.items():
        if isinstance(v, (datetime.datetime, datetime.date)):
            v = v.isoformat()
        _data[k] = v
    return _data


@commands.create_changelog_entry.register()
async def create_changelog_entry(
    context: Context,
    model: (Model, Property),
    backend: PostgreSQL,
    *,
    dstream: types.AsyncGeneratorType,
) -> None:
    transaction = context.get('transaction')
    connection = transaction.connection
    if isinstance(model, Model):
        table = backend.tables[model.manifest.name][model.name]
    else:
        table = backend.tables[model.model.manifest.name][model.model.name]
    async for data in dstream:
        qry = table.changes.insert().values(
            transaction=transaction.id,
            datetime=utcnow(),
            action=Action.INSERT.value,
        )
        connection.execute(qry, [{
            'id': data.saved['_id'] if data.saved else data.patch['_id'],
            '_revision': data.patch['_revision'] if data.patch else data.saved['_revision'],
            'transaction': transaction.id,
            'datetime': utcnow(),
            'action': data.action.value,
            'data': _fix_data_for_json({
                k: v for k, v in data.patch.items() if not k.startswith('_')
            }),
        }])
        yield data
