from typing import AsyncIterator, Optional, List, Union, Tuple, Iterator

import contextlib
import datetime
import enum
import hashlib
import itertools
import logging
import typing
import types
import cgi
import uuid

import jsonpatch
import sqlalchemy as sa
import sqlalchemy.exc
from ruamel.yaml import YAML
from sqlalchemy.dialects.postgresql import JSONB, UUID, BIGINT, ARRAY
from sqlalchemy.engine.result import RowProxy
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import FunctionElement
from sqlalchemy.sql.type_api import TypeEngine
from starlette.requests import Request
from starlette.responses import Response
from multipledispatch import dispatch

from spinta import commands
from spinta import exceptions
from spinta.backends import Backend, BackendFeatures
from spinta.commands import wait, load, prepare, migrate, getone, getall, wipe, authorize
from spinta.components import Context, Manifest, Model, Property, Action, UrlParams, DataItem, DataSubItem, Store
from spinta.config import RawConfig
from spinta.hacks.recurse import _replace_recurse
from spinta.renderer import render
from spinta.utils.schema import NA, is_valid_sort_key
from spinta.utils.json import fix_data_for_json
from spinta.utils.aiotools import aiter
from spinta.utils.data import take
from spinta.commands.write import prepare_patch, simple_response, validate_data
from spinta.backends.postgresql.files import DatabaseFile
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


log = logging.getLogger(__name__)


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
    FILE = '/:file'


class PostgreSQL(Backend):
    metadata = {
        'name': 'postgresql',
        'properties': {
            'dsn': {'type': 'string', 'required': True},
        },
    }

    features = {
        BackendFeatures.FILE_BLOCKS,
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
                    table.insert().values(
                        # FIXME: commands.gen_object_id should be used here
                        _id=str(uuid.uuid4()),
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

    def add_table(
        self,
        table: sa.Table,
        node: Union[Model, Property],
        ttype: TableType = TableType.MAIN,
    ):
        if node.manifest.name not in self.tables:
            self.tables[node.manifest.name] = {}
        tables = self.tables[node.manifest.name]
        name = get_table_name(node, ttype)
        assert name not in tables, name
        tables[name] = table

    def get_table(
        self,
        node: Union[Model, Property],
        ttype: TableType = TableType.MAIN,
        *,
        fail: bool = True,
    ):
        name = get_table_name(node, ttype)
        if fail:
            return self.tables[node.manifest.name][name]
        else:
            return self.tables.get(node.manifest.name, {}).get(name)


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
        # FIXME: _revision should has its own type and on database column type
        #        should bet received from get_primary_key_type() command.
        if prop.name.startswith('_') and prop.name not in ('_id', '_revision'):
            continue
        column = prepare(context, backend, prop)
        if isinstance(column, list):
            columns.extend(column)
        elif column is not None:
            columns.append(column)

    # Create main table.
    main_table_name = get_pg_name(get_table_name(model))
    pkey_type = commands.get_primary_key_type(context, backend)
    main_table = sa.Table(
        main_table_name, backend.schema,
        sa.Column('_txn', pkey_type, sa.ForeignKey('transaction._id')),
        sa.Column('_created', sa.DateTime),
        sa.Column('_updated', sa.DateTime),
        *columns,
    )
    backend.add_table(main_table, model)

    # Create changes table.
    changelog_table = get_changes_table(context, backend, model)
    backend.add_table(changelog_table, model, TableType.CHANGELOG)


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
    elif dtype.name == 'binary':
        return sa.Column(name, sa.LargeBinary)
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


@commands.get_primary_key_type.register()
def get_primary_key_type(context: Context, backend: PostgreSQL):
    return UUID()


@prepare.register()
def prepare(context: Context, backend: PostgreSQL, dtype: PrimaryKey):
    pkey_type = commands.get_primary_key_type(context, backend)
    return sa.Column('_id', pkey_type, primary_key=True)


@prepare.register()
def prepare(context: Context, backend: PostgreSQL, dtype: Ref):
    # TODO: rename dtype.object to dtype.model
    ref_model = commands.get_referenced_model(context, dtype.prop, dtype.object)
    pkey_type = commands.get_primary_key_type(context, backend)
    return get_pg_foreign_key(
        dtype.prop,
        table_name=get_pg_name(get_table_name(ref_model)),
        model_name=dtype.prop.model.name,
        column_type=pkey_type,
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
            name=get_pg_name(f'fk_{model_name}_{column_name}'),
        )
    ]


@prepare.register()
def prepare(context: Context, backend: PostgreSQL, dtype: File):
    prop = dtype.prop
    model = prop.model

    # All properties receive model backend even if properties own backend
    # differs. Its properties responsibility to deal with foreign backends.
    assert model.backend.name == backend.name

    pkey_type = commands.get_primary_key_type(context, backend)

    if prop.backend.name == backend.name:
        # If property is on the same backend currently being prepared, then
        # create table for storing file blocks and also add file metadata
        # columns.
        table_name = get_pg_name(get_table_name(prop, TableType.FILE))
        table = sa.Table(
            table_name, backend.schema,
            sa.Column('_id', pkey_type, primary_key=True),
            sa.Column('_block', sa.LargeBinary),
        )
        backend.add_table(table, prop, TableType.FILE)

    name = _get_column_name(prop)

    # Required file metadata on any backend.
    columns = [
        sa.Column(f'{name}._id', sa.String),
        sa.Column(f'{name}._content_type', sa.String),
        sa.Column(f'{name}._size', BIGINT),
    ]

    # Optional file metadata, depending on backend supported features.
    if BackendFeatures.FILE_BLOCKS in prop.backend.features:
        columns += [
            sa.Column(f'{name}._bsize', sa.Integer),
            sa.Column(f'{name}._blocks', ARRAY(pkey_type)),
        ]

    return columns


@prepare.register()
def prepare(context: Context, backend: PostgreSQL, dtype: Array):
    prop = dtype.prop

    columns = prepare(context, backend, dtype.items)
    assert columns is not None
    if not isinstance(columns, list):
        columns = [columns]

    pkey_type = commands.get_primary_key_type(context, backend)

    # TODO: When all list items will have unique id, also add reference to
    #       parent list id.
    # if prop.list:
    #     parent_list_table_name = get_pg_name(
    #         get_table_name(prop.list, TableType.LIST)
    #     )
    #     columns += [
    #         # Parent list table id.
    #         sa.Column('_lid', pkey_type, sa.ForeignKey(
    #             f'{parent_list_table_name}._id', ondelete='CASCADE',
    #         )),
    #     ]

    name = get_pg_name(get_table_name(prop, TableType.LIST))
    main_table_name = get_pg_name(get_table_name(prop.model))
    table = sa.Table(
        name, backend.schema,
        # TODO: List tables eventually will have _id in order to uniquelly
        #       identify list item.
        # sa.Column('_id', pkey_type, primary_key=True),
        sa.Column('_txn', pkey_type, sa.ForeignKey('transaction._id')),
        # Main table id (resource id).
        sa.Column('_rid', pkey_type, sa.ForeignKey(
            f'{main_table_name}._id', ondelete='CASCADE',
        )),
        *columns,
    )
    backend.add_table(table, prop, TableType.LIST)

    if prop.list is None:
        # For fast whole resource access we also store whole list in a JSONB.
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


@migrate.register()
def migrate(context: Context, backend: PostgreSQL):
    # XXX: I found, that this some times leaks connection, you can check that by
    #      comparing `backend.engine.pool.checkedin()` before and after this
    #      line.
    # TODO: update appropriate rows in _schema and save `applied` date
    #       of schema migration
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
    table = backend.get_table(prop)

    if (
        data.action in (Action.UPDATE, Action.PATCH) or
        data.action == Action.UPSERT and data.saved is not NA
    ):
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
    backend: PostgreSQL,
    action: Action,
    pk: str,
    patch: dict,
) -> None:
    transaction = context.get('transaction')
    connection = transaction.connection
    rows = _get_lists_data(model, patch)
    sort_key = lambda x: x[0].place  # noqa
    rows = sorted(rows, key=sort_key)
    for place, rows in itertools.groupby(rows, key=sort_key):
        prop = model.flatprops[place]
        table = backend.get_table(prop, TableType.LIST)
        if action != Action.INSERT:
            connection.execute(table.delete().where(table.c._rid == pk))
        rows = [
            {
                '_txn': transaction.id,
                '_rid': pk,
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


@commands.push.register()
async def push(
    context: Context,
    request: Request,
    dtype: File,
    backend: PostgreSQL,
    *,
    action: Action,
    params: UrlParams,
):
    if params.propref:
        return await push[type(context), Request, DataType, type(backend)](
            context, request, dtype, backend,
            action=action,
            params=params,
        )

    prop = dtype.prop

    # XXX: This command should just prepare AsyncIterator[DataItem] and call
    #      push_stream or something like that. Now I think this command does
    #      too much work.

    authorize(context, action, prop)

    data = DataItem(
        prop.model,
        prop,
        propref=False,
        backend=backend,
        action=action
    )

    if action == Action.DELETE:
        data.given = {
            prop.name: {
                '_id': None,
                '_content_type': None,
                '_content': None,
            }
        }
    else:
        data.given = {
            prop.name: {
                '_content_type': request.headers.get('content-type'),
                '_content': await request.body(),
            }
        }
        if 'Content-Disposition' in request.headers:
            _, cdisp = cgi.parse_header(request.headers['Content-Disposition'])
            if 'filename' in cdisp:
                data.given[prop.name]['_id'] = cdisp['filename']

    if 'Revision' in request.headers:
        data.given['_revision'] = request.headers['Revision']

    commands.simple_data_check(context, data, data.prop, data.model.backend)

    data.saved = getone(context, prop, dtype, prop.model.backend, id_=params.pk)

    dstream = aiter([data])
    dstream = validate_data(context, dstream)
    dstream = prepare_patch(context, dstream)

    if action in (Action.UPDATE, Action.PATCH, Action.DELETE):
        dstream = commands.update(context, prop, dtype, prop.model.backend, dstream=dstream)
        dstream = commands.create_changelog_entry(
            context, prop.model, prop.model.backend, dstream=dstream,
        )

    elif action == Action.DELETE:
        dstream = commands.delete(context, prop, dtype, prop.model.backend, dstream=dstream)
        dstream = commands.create_changelog_entry(
            context, prop.model, prop.model.backend, dstream=dstream,
        )

    else:
        raise Exception(f"Unknown action {action!r}.")

    status_code, response = await simple_response(context, dstream)
    return render(context, request, prop, params, response, status_code=status_code)


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
    table = backend.get_table(model)
    async for data in dstream:
        patch = commands.before_write(context, model, backend, data=data)

        # TODO: Refactor this to insert batches with single query.
        qry = table.insert().values(
            _id=patch['_id'],
            _revision=patch['_revision'],
            _txn=transaction.id,
            _created=utcnow(),
        )
        connection.execute(qry, patch)

        commands.after_write(context, model, backend, data=data)

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
    table = backend.get_table(model)

    async for data in dstream:
        if not data.patch:
            yield data
            continue

        pk = data.saved['_id']
        patch = commands.before_write(context, model, backend, data=data)
        result = connection.execute(
            table.update().
            where(table.c._id == pk).
            where(table.c._revision == data.saved['_revision']).
            values(patch)
        )

        if result.rowcount == 0:
            raise Exception(f"Update failed, {model} with {pk} not found.")
        elif result.rowcount > 1:
            raise Exception(
                f"Update failed, {model} with {pk} has found and update "
                f"{result.rowcount} rows."
            )

        commands.after_write(context, model, backend, data=data)

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
    table = backend.get_table(model)
    async for data in dstream:
        commands.before_write(context, model, backend, data=data)
        connection.execute(
            table.delete().
            where(table.c._id == data.saved['_id'])
        )
        commands.after_write(context, model, backend, data=data)
        yield data


@commands.before_write.register()
def before_write(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    data: DataSubItem,
) -> dict:
    patch = take(['_id'], data.patch)
    patch['_revision'] = take('_revision', data.patch, data.saved)
    patch['_txn'] = context.get('transaction').id
    patch['_created'] = utcnow()
    for prop in take(model.properties).values():
        value = commands.before_write(
            context,
            prop.dtype,
            backend,
            data=data[prop.name],
        )
        patch.update(value)
    return patch


@commands.after_write.register()
def after_write(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    data: DataSubItem,
) -> dict:
    for key in take(data.patch or {}):
        prop = model.properties[key]
        commands.after_write(context, prop.dtype, backend, data=data[key])


@commands.before_write.register()
def before_write(  # noqa
    context: Context,
    dtype: Array,
    backend: PostgreSQL,
    *,
    data: DataSubItem,
):
    if data.saved and data.patch is not NA:
        prop = dtype.prop
        table = backend.get_table(prop, TableType.LIST)
        transaction = context.get('transaction')
        connection = transaction.connection
        connection.execute(
            table.delete().
            where(table.c._rid == data.root.saved['_id'])
        )

    if dtype.prop.list:
        return {}
    else:
        return take({dtype.prop.place: data.patch})


@commands.after_write.register()
def after_write(  # noqa
    context: Context,
    dtype: Array,
    backend: PostgreSQL,
    *,
    data: DataSubItem,
):
    if data.patch:
        prop = dtype.prop
        table = backend.get_table(prop, TableType.LIST)
        transaction = context.get('transaction')
        connection = transaction.connection
        rid = take('_id', data.root.patch, data.root.saved)
        rows = [
            {
                _get_list_column_name(prop.place, k): v
                for k, v in commands.before_write(
                    context,
                    dtype.items.dtype,
                    backend,
                    data=d,
                ).items()
            }
            for d in data.iter(patch=True)
        ]
        qry = table.insert().values({
            '_txn': transaction.id,
            '_rid': rid,
        })
        connection.execute(qry, rows)

        for d in data.iter(patch=True):
            commands.after_write(context, dtype.items.dtype, backend, data=d)


@commands.before_write.register()
def before_write(  # noqa
    context: Context,
    dtype: File,
    backend: PostgreSQL,
    *,
    data: DataSubItem,
):
    content = take('_content', data.patch)
    if isinstance(content, bytes):
        transaction = context.get('transaction')
        connection = transaction.connection
        prop = dtype.prop
        table = backend.get_table(prop, TableType.FILE)
        with DatabaseFile(connection, table, mode='w') as f:
            f.write(data.patch['_content'])
            data.patch['_size'] = f.size
            data.patch['_blocks'] = f.blocks
            data.patch['_bsize'] = f.bsize

    return commands.before_write[type(context), File, Backend](
        context,
        dtype,
        backend,
        data=data,
    )


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
    data = commands.prepare_data_for_response(
        context,
        Action.GETONE,
        model,
        backend,
        data,
        select=params.select,
    )
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


@getone.register()
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
    authorize(context, action, prop)
    data = getone(context, prop, dtype, backend, id_=params.pk)
    data = commands.prepare_data_for_response(
        context,
        Action.GETONE,
        prop.dtype,
        backend,
        data,
    )
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
        yield _get_column_name(dtype.prop)


@dispatch(File)
def _iter_prop_names(dtype) -> Iterator[Property]:  # noqa
    yield dtype.prop.place + '._id'
    yield dtype.prop.place + '._content_type'


@getone.register()
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
    authorize(context, action, prop)
    data = getone(context, prop, dtype, backend, id_=params.pk)

    # Return file metadata
    if params.propref:
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


@getone.register()
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

    if BackendFeatures.FILE_BLOCKS in prop.backend.features:
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
        commands.prepare_data_for_response(
            context,
            action,
            model,
            backend,
            row,
            select=params.select,
        )
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
        select: typing.List[str] = None,
        sort: typing.Dict[str, dict] = None,
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
            field = list_table.c[_get_column_name(prop)]
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
        sort: typing.List[typing.Tuple[str, str]],
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
    table = backend.get_table(model, TableType.CHANGELOG)

    qry = sa.select([table]).order_by(table.c._id)
    qry = _changes_id(table, qry, id_)
    qry = _changes_offset(table, qry, offset)
    qry = _changes_limit(qry, limit)

    result = connection.execute(qry)
    for row in result:
        yield {
            '_id': row[table.c._id],
            '_revision': row[table.c._revision],
            '_txn': row[table.c._txn],
            '_rid': row[table.c._rid],
            '_created': row[table.c.datetime],
            '_op': row[table.c.action],
            **dict(row[table.c.data]),
        }


def _changes_id(table, qry, id_):
    if id_:
        return qry.where(table.c._rid == id_)
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
    table = backend.get_table(model, fail=False)
    if table is None:
        # Model backend might not be prepared, this is especially true for
        # tests. So if backend is not yet prepared, just skipt this model.
        return

    for prop in model.properties.values():
        wipe(context, prop.dtype, backend)

    connection = context.get('transaction').connection

    table = backend.get_table(model, TableType.CHANGELOG)
    connection.execute(table.delete())

    table = backend.get_table(model)
    connection.execute(table.delete())


@wipe.register()
def wipe(context: Context, dtype: DataType, backend: PostgreSQL):
    pass


@wipe.register()
def wipe(context: Context, dtype: Object, backend: PostgreSQL):
    for prop in dtype.properties.values():
        wipe(context, prop.dtype, backend)


@wipe.register()
def wipe(context: Context, dtype: Array, backend: PostgreSQL):
    wipe(context, dtype.items.dtype, backend)
    table = backend.get_table(dtype.prop, TableType.LIST)
    connection = context.get('transaction').connection
    connection.execute(table.delete())


class utcnow(FunctionElement):
    type = sa.DateTime()


@compiles(utcnow, 'postgresql')
def pg_utcnow(element, compiler, **kw):
    return "TIMEZONE('utc', CURRENT_TIMESTAMP)"


def get_table_name(
    node: Union[Model, Property],
    ttype: TableType = TableType.MAIN,
) -> str:
    # XXX: Dataset models is going to be deleted soon (time of this comment:
    #      2020-01-31)
    from spinta.types import dataset
    if isinstance(node, (Model, dataset.Model)):
        model = node
    else:
        model = node.model
    if ttype in (TableType.LIST, TableType.FILE):
        name = model.model_type() + ttype.value + '/' + node.place
    else:
        name = model.model_type() + ttype.value
    return name


def get_pg_name(name: str) -> str:
    if len(name) > NAMEDATALEN:
        hs = 8
        h = hashlib.sha1(name.encode()).hexdigest()[:hs]
        i = int(NAMEDATALEN / 100 * 60)
        j = NAMEDATALEN - i - hs - 2
        name = name[:i] + '_' + h + '_' + name[-j:]
    return name


def get_changes_table(context: Context, backend: PostgreSQL, model: Model):
    table_name = get_pg_name(get_table_name(model, TableType.CHANGELOG))
    pkey_type = commands.get_primary_key_type(context, backend)
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
        sa.Column('_id', BIGINT, primary_key=True),
        # FIXME: Should be pkey_type, but String is used, because dataset models
        #        use sha1 for resource ids.
        sa.Column('_revision', sa.String),
        sa.Column('_txn', pkey_type, sa.ForeignKey('transaction._id')),
        # FIXME: Should be pkey_type, but String is used, because dataset models
        #        use sha1 for resource ids.
        sa.Column('_rid', sa.String),  # reference to main table
        sa.Column('datetime', sa.DateTime),
        # FIXME: Change `action` to `_op` for consistency.
        sa.Column('action', sa.String(8)),  # insert, update, delete
        sa.Column('data', JSONB),
    )
    return table


@commands.prepare_data_for_response.register()
def prepare_data_for_response(
    context: Context,
    action: Action,
    model: Model,
    backend: PostgreSQL,
    value: RowProxy,
    *,
    select: typing.List[str] = None,
) -> dict:
    return commands.prepare_data_for_response(
        context,
        action,
        model,
        backend,
        dict(value),
        select=select,
    )


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


@commands.create_changelog_entry.register()
async def create_changelog_entry(
    context: Context,
    node: (Model, Property),
    backend: PostgreSQL,
    *,
    dstream: types.AsyncGeneratorType,
) -> None:
    transaction = context.get('transaction')
    connection = transaction.connection
    table = backend.get_table(node, TableType.CHANGELOG)
    async for data in dstream:
        qry = table.insert().values(
            _txn=transaction.id,
            datetime=utcnow(),
            action=Action.INSERT.value,
        )
        connection.execute(qry, [{
            '_rid': data.saved['_id'] if data.saved else data.patch['_id'],
            '_revision': data.patch['_revision'] if data.patch else data.saved['_revision'],
            '_txn': transaction.id,
            'datetime': utcnow(),
            'action': data.action.value,
            'data': fix_data_for_json({
                k: v for k, v in data.patch.items() if not k.startswith('_')
            }),
        }])
        yield data
