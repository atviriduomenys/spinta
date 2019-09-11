import contextlib
import datetime
import hashlib
import itertools
import re
import typing

import unidecode
import sqlalchemy as sa
import sqlalchemy.exc
from sqlalchemy.dialects.postgresql import JSONB, BIGINT, UUID
from sqlalchemy.engine.result import RowProxy
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import FunctionElement
from starlette.requests import Request
from starlette.exceptions import HTTPException

from spinta import commands
from spinta.auth import check_scope
from spinta.backends import Backend, check_model_properties, check_type_value
from spinta.commands import wait, load, prepare, migrate, check, push, getone, getall, wipe, authorize, dump, gen_object_id
from spinta.common import NA
from spinta.components import Context, Manifest, Model, Property, Action, UrlParams
from spinta.config import RawConfig
from spinta.renderer import render
from spinta.types.type import Type, File, PrimaryKey, Ref
from spinta.utils.changes import get_patch_changes
from spinta.utils.idgen import get_new_id
from spinta.utils.response import get_request_data

from spinta.exceptions import (
    MultipleRowsFound,
    NotFoundError,
    RevisionError,
    ResourceNotFoundError,
    UniqueConstraintError,
)

# Maximum length for PostgreSQL identifiers (e.g. table names, column names,
# function names).
# https://github.com/postgres/postgres/blob/master/src/include/pg_config_manual.h
NAMEDATALEN = 63


PG_CLEAN_NAME_RE = re.compile(r'[^a-z0-9]+', re.IGNORECASE)

MAIN_TABLE = 'M'
CHANGES_TABLE = 'C'
CACHE_TABLE = 'T'

UNSUPPORTED_TYPES = [
    'backref',
    'generic',
    'array',
    'object',
]


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
            for model in resource.objects.values():
                if model.backend.name == backend.name:
                    prepare(context, backend, model)


@prepare.register()
def prepare(context: Context, backend: PostgreSQL, model: Model):
    columns = []
    for prop in model.properties.values():
        column = prepare(context, backend, prop)
        if isinstance(column, list):
            columns.extend(column)
        elif column is not None:
            columns.append(column)

    # Create main table.
    main_table_name = get_table_name(backend, model.manifest.name, model.name, MAIN_TABLE)
    main_table = sa.Table(
        main_table_name, backend.schema,
        sa.Column('_transaction_id', sa.Integer, sa.ForeignKey('transaction.id')),
        *columns,
    )

    # Create changes table.
    changes_table_name = get_table_name(backend, model.manifest.name, model.name, CHANGES_TABLE)
    # XXX: not sure if I should pass main_table.c.id.type.__class__() or a
    #      shorter form.
    changes_table = get_changes_table(backend, changes_table_name, main_table.c.id.type)

    backend.tables[model.manifest.name][model.name] = ModelTables(main_table, changes_table)


@prepare.register()
def prepare(context: Context, backend: PostgreSQL, type: Type):
    if type.prop.backend.name != backend.name:
        # If property backend differs from modle backend, then no columns should
        # be added to the table. If some property types requires adding column
        # even for a property with different backend, then this type must
        # implement prepare command add do custom logic there.
        return

    if type.name == 'type':
        return
    elif type.name == 'string':
        return sa.Column(type.prop.name, sa.Text)
    elif type.name == 'date':
        return sa.Column(type.prop.name, sa.Date)
    elif type.name == 'datetime':
        return sa.Column(type.prop.name, sa.DateTime)
    elif type.name == 'integer':
        return sa.Column(type.prop.name, sa.Integer)
    elif type.name == 'number':
        return sa.Column(type.prop.name, sa.Float)
    elif type.name == 'boolean':
        return sa.Column(type.prop.name, sa.Boolean)
    elif type.name in ('spatial', 'image'):
        # TODO: these property types currently are not implemented
        return sa.Column(type.prop.name, sa.Text)
    elif type.name in UNSUPPORTED_TYPES:
        return
    else:
        raise Exception(f"Unknown property type {type.name!r}.")


@prepare.register()
def prepare(context: Context, backend: PostgreSQL, type: PrimaryKey):
    if type.prop.manifest.name == 'internal':
        return sa.Column(type.prop.name, BIGINT, primary_key=True)
    else:
        return sa.Column(type.prop.name, UUID(), primary_key=True)


@prepare.register()
def prepare(context: Context, backend: PostgreSQL, type: Ref):
    ref_model = type.prop.model.manifest.objects['model'][type.object]
    table_name = get_table_name(backend, ref_model.manifest.name, ref_model.name)
    if ref_model.manifest.name == 'internal':
        column_type = sa.Integer()
    else:
        column_type = UUID()
    return [
        sa.Column(type.prop.name, column_type),
        sa.ForeignKeyConstraint(
            [type.prop.name], [f'{table_name}.id'],
            name=_get_pg_name(f'fk_{type.prop.model.name}_{type.prop.name}'),
        )
    ]


@prepare.register()
def prepare(context: Context, backend: PostgreSQL, type: File):
    if type.prop.backend.name == backend.name:
        return sa.Column(type.prop.name, sa.LargeBinary)
    else:
        # If file property has a different backend, then here we just need to
        # save file name of file stored externally.
        return sa.Column(type.prop.name, JSONB)


def _get_pg_name(name):
    if len(name) > NAMEDATALEN:
        name_hash = hashlib.sha256(name.encode()).hexdigest()
        return name[:NAMEDATALEN - 7] + '_' + name_hash[-6:]
    else:
        return name


class ModelTables(typing.NamedTuple):
    main: sa.Table = None
    changes: sa.Table = None
    cache: sa.Table = None


@migrate.register()
def migrate(context: Context, backend: PostgreSQL):
    # XXX: I found, that this some times leaks connection, you can check that by
    #      comparing `backend.engine.pool.checkedin()` before and after this
    #      line.
    backend.schema.create_all(checkfirst=True)


@check.register()
def check(context: Context, model: Model, backend: PostgreSQL, data: dict, *, action: Action, id_: str):
    check_model_properties(context, model, backend, data, action, id_)


@check.register()
def check(context: Context, type_: Type, prop: Property, backend: PostgreSQL, value: object, *, data: dict, action: Action):
    check_type_value(type_, value)

    connection = context.get('transaction').connection
    table = backend.tables[prop.manifest.name][prop.model.name].main

    if type_.unique and value is not NA:
        if action == Action.UPDATE:
            condition = sa.and_(
                table.c[prop.name] == value,
                table.c['id'] != data['id'],
            )
        # PATCH requests are allowed to send protected fields in requests JSON
        # PATCH handling will use those fields for validating data, though
        # won't change them.
        elif action == Action.PATCH and type_.prop.name in {'id', 'type', 'revision'}:
            return
        else:
            condition = table.c[prop.name] == value
        not_found = object()
        result = backend.get(connection, table.c[prop.name], condition, default=not_found)
        if result is not not_found:
            raise UniqueConstraintError(model=prop.model.name, prop=prop.place)


@check.register()
def check(context: Context, type: File, prop: Property, backend: PostgreSQL, value: dict, *, data: dict, action: Action):
    if prop.backend.name != backend.name:
        check(context, type, prop, prop.backend, value, data=data, action=action)


@push.register()
async def push(
    context: Context,
    request: Request,
    model: Model,
    backend: PostgreSQL,
    *,
    action: Action,
    params: UrlParams,
):
    authorize(context, action, model)

    if action == Action.DELETE:
        data = {}
    else:
        data = await get_request_data(request)
        data = load(context, model, data)
        check(context, model, backend, data, action=action, id_=params.id)
        data = prepare(context, model, data, action=action)
        data = {
            k: v
            for k, v in data.items()
            if model.properties[k].type.name not in UNSUPPORTED_TYPES
        }

    if action == Action.INSERT:
        data['id'] = commands.insert(context, model, backend, data=data)

    elif action == Action.UPSERT:
        data['id'] = commands.upsert(context, model, backend, data=data)

    elif action == Action.UPDATE:
        commands.update(context, model, backend, id_=params.id, data=data)
        data['id'] = params.id

    elif action == Action.PATCH:
        commands.patch(context, model, backend, id_=params.id, data=data)
        data['id'] = params.id

    elif action == Action.DELETE:
        commands.delete(context, model, backend, id_=params.id)
        data['id'] = params.id

    else:
        raise Exception(f"Unknown action {action!r}.")

    data = prepare(context, action, model, backend, data)

    if action == Action.INSERT:
        status_code = 201
    elif action == Action.DELETE:
        status_code = 204
    else:
        status_code = 200

    return render(context, request, model, action, params, data, status_code=status_code)


@commands.insert.register()
def insert(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    data: dict,
):
    transaction = context.get('transaction')
    connection = transaction.connection
    table = backend.tables[model.manifest.name][model.name]

    if 'id' in data:
        check_scope(context, 'set_meta_fields')

    if 'revision' in data:
        # FIXME: revision should have model for context
        raise RevisionError()

    if not data.get('id'):
        data['id'] = gen_object_id(context, backend, model)

    # FIXME: before creating revision check if there's no collision clash
    data['revision'] = get_new_id('revision id')

    connection.execute(
        table.main.insert().values(data),
    )

    # Track changes.
    connection.execute(
        table.changes.insert().values(
            transaction_id=transaction.id,
            id=data['id'],
            datetime=utcnow(),
            action=Action.INSERT.value,
            change=_fix_data_for_json({
                k: v for k, v in data.items() if k not in {'id'}
            }),
        ),
    )

    return data['id']


@commands.upsert.register()
def upsert(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    key: typing.List[str],
    data: dict,
):
    transaction = context.get('transaction')
    connection = transaction.connection
    table = backend.tables[model.manifest.name][model.name]

    condition = []
    for k in key:
        condition.append(table.main.c[k] == data[k])

    row = backend.get(connection, table.main, sa.and_(*condition), default=None)

    if row is None:
        action = Action.INSERT

        if 'id' not in data:
            data['id'] = gen_object_id(context, backend, model)

        if 'revision' in data.keys():
            # FIXME: revision should have model for context
            raise RevisionError()
        data['revision'] = get_new_id('revision id')

        connection.execute(
            table.main.insert().values(data),
        )

    else:
        action = Action.PATCH

        id_ = row[table.main.c.id]

        # FIXME: before creating revision check if there's no collision clash
        data['revision'] = get_new_id('revision id')
        data = _patch(transaction, connection, table, id_, row, data)

        if data is None:
            # Nothing changed.
            return None

    # Track changes.
    connection.execute(
        table.changes.insert().values(
            transaction_id=transaction.id,
            id=id_,
            datetime=utcnow(),
            action=action.value,
            change=_fix_data_for_json(data),
        ),
    )

    return id_


@commands.update.register()
def update(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    id_: str,
    data: dict,
):
    transaction = context.get('transaction')
    connection = transaction.connection
    table = backend.tables[model.manifest.name][model.name]

    result = connection.execute(
        table.main.update().
        where(table.main.c.id == id_).
        values(data)
    )
    if result.rowcount == 0:
        raise Exception("Update failed, {model} with {id_} not found.")
    elif result.rowcount > 1:
        raise Exception("Update failed, {model} with {id_} has found and update {result.rowcount} rows.")

    # Track changes.
    connection.execute(
        table.changes.insert().values(
            transaction_id=transaction.id,
            id=data['id'],
            datetime=utcnow(),
            action=Action.UPDATE.value,
            change=_fix_data_for_json({
                k: v for k, v in data.items() if k not in {'id'}
            }),
        ),
    )


@commands.patch.register()
def patch(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    id_: str,
    data: dict,
):
    transaction = context.get('transaction')
    connection = transaction.connection
    table = backend.tables[model.manifest.name][model.name]

    row = backend.get(
        connection,
        [table.main],
        table.main.c.id == id_,
        default=None,
    )
    if row is None:
        type_ = model.get_type_value()
        raise ResourceNotFoundError(model=type_, id_=id_)

    # FIXME: before creating revision check if there's no collision clash
    data['revision'] = get_new_id('revision id')
    data = _patch(transaction, connection, table, id_, row, data)

    if data is None:
        # Nothing changed.
        return None

    # Track changes.
    connection.execute(
        table.changes.insert().values(
            transaction_id=transaction.id,
            id=id_,
            datetime=utcnow(),
            action=Action.PATCH.value,
            change=_fix_data_for_json(data),
        ),
    )


def _patch(transaction, connection, table, id_, row, data):
    changes = get_patch_changes(dict(row), data)

    if not changes:
        # Nothing to update.
        return None

    result = connection.execute(
        table.main.update().
        where(table.main.c.id == id_).
        where(table.main.c._transaction_id == row[table.main.c._transaction_id]).
        values(changes)
    )

    # TODO: Retries are needed if result.rowcount is 0, if such
    #       situation happens, that means a concurrent transaction
    #       changed the data and we need to reread it.
    #
    #       And assumption is made here, than in the same
    #       transaction there are no concurrent updates, if this
    #       assumption is false, then we need to check against
    #       change_id instead of transaction_id.
    assert result.rowcount > 0

    return changes


@commands.delete.register()
def delete(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    id_: str,
):
    transaction = context.get('transaction')
    connection = transaction.connection
    table = backend.tables[model.manifest.name][model.name]

    res = connection.execute(
        table.main.delete().
        where(table.main.c.id == id_)
    )
    if res.rowcount == 0:
        raise ResourceNotFoundError(model=model.name, id_=id_)

    # Track changes.
    connection.execute(
        table.changes.insert().values(
            transaction_id=transaction.id,
            id=id_,
            datetime=utcnow(),
            action=Action.DELETE.value,
            change=None,
        ),
    )


@push.register()
async def push(
    context: Context,
    request: Request,
    prop: Property,
    backend: PostgreSQL,
    *,
    action: Action,
    params: UrlParams,
    ref: bool = False,
):
    """

    Args:
        ref: Update reference or data refered by reference.

            This applies only to direct property updates. Update reference is
            enabled, when property is named like this `prop:ref`, the `:ref`
            suffix tells, that reference should be updated and `ref` argument is
            set to True.

            Most properties do not have references, but some do. So this only
            applies to properties like File with external backend, ForeignKey
            and etc.

    """
    if action == Action.INSERT:
        raise HTTPException(status_code=400, detail=f"Can't POST to a property, use PUT or PATCH instead.")

    authorize(context, action, prop)

    data = await get_request_data(request)

    data = load(context, prop.type, data)
    check(context, prop.type, prop, backend, data, data=None, action=action)
    data = prepare(context, prop.type, backend, data)

    if action == Action.UPDATE:
        commands.update(context, prop, backend, id_=params.id, data=data)
    elif action == Action.PATCH:
        commands.patch(context, prop, backend, id_=params.id, data=data)
    elif action == Action.DELETE:
        commands.delete(context, prop, backend, id_=params.id)
    else:
        raise Exception(f"Unknown action {action}.")

    data = dump(context, backend, prop.type, data)
    return render(context, request, prop, action, params, data)


@commands.update.register()  # noqa
def update(
    context: Context,
    prop: Property,
    backend: PostgreSQL,
    *,
    id_: str,
    data: dict,
):
    connection = context.get('transaction').connection
    table = backend.tables[prop.model.manifest.name][prop.model.name]
    result = connection.execute(
        table.main.update().
        where(table.main.c.id == id_).
        values({prop.name: data})
    )
    if result.rowcount == 0:
        raise Exception("Property update failed, {prop} with {params.id} not found.")
    elif result.rowcount > 1:
        raise Exception("Property update failed, {prop} with {params.id} has found and update {result.rowcount} rows.")


@commands.patch.register()  # noqa
def patch(
    context: Context,
    prop: Property,
    backend: PostgreSQL,
    *,
    id_: str,
    data: dict,
):
    connection = context.get('transaction').connection
    table = backend.tables[prop.model.manifest.name][prop.model.name]
    result = connection.execute(
        table.main.update().
        where(table.main.c.id == id_).
        values({prop.name: data})
    )
    if result.rowcount == 0:
        raise Exception("Property update failed, {prop} with {params.id} not found.")
    elif result.rowcount > 1:
        raise Exception("Property update failed, {prop} with {params.id} has found and update {result.rowcount} rows.")


@commands.delete.register()  # noqa
def delete(
    context: Context,
    prop: Property,
    backend: PostgreSQL,
    *,
    id_: str,
):
    connection = context.get('transaction').connection
    table = backend.tables[prop.model.manifest.name][prop.model.name]
    result = connection.execute(
        table.main.update().
        where(table.main.c.id == id_).
        values({prop.name: None})
    )
    if result.rowcount == 0:
        raise Exception("Property delete failed, {prop} with {params.id} not found.")
    elif result.rowcount > 1:
        raise Exception("Property delete failed, {prop} with {params.id} has found and update {result.rowcount} rows.")


@getone.register()
async def getone(
    context: Context,
    request: Request,
    model: Model,
    backend: PostgreSQL,
    *,
    action: Action,
    params: UrlParams,
    ref: bool = False,
):
    authorize(context, action, model)
    data = getone(context, model, backend, id_=params.id)
    data = prepare(context, Action.GETONE, model, backend, data, show=params.show)
    return render(context, request, model, action, params, data)


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
    result = backend.get(connection, table, table.c.id == id_)
    return dict(result)


@getone.register()
async def getone(
    context: Context,
    request: Request,
    prop: Property,
    backend: PostgreSQL,
    *,
    action: Action,
    params: UrlParams,
    ref: bool = False,
):
    authorize(context, action, prop)
    data = getone(context, prop, backend, id_=params.id)
    data = dump(context, backend, prop.type, data)
    return render(context, request, prop, action, params, data)


@getone.register()
def getone(
    context: Context,
    prop: Property,
    backend: PostgreSQL,
    *,
    id_: str,
):
    table = backend.tables[prop.manifest.name][prop.model.name].main
    connection = context.get('transaction').connection
    return backend.get(connection, table.c[prop.name], table.c.id == id_)


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
    # TODO: add all the filtering support to postgresql
    # show=_params.get('show'),
    # sort=_params.get('sort', [{'name': 'id', 'ascending': True}]),
    # offset=_params.get('offset'),
    # limit=_params.get('limit', 100),
    # count='count' in _params,
    # query_params=_params.get('query_params'),
    # search=params.search,

    authorize(context, action, model)
    result = getall(context, model, backend)
    return render(context, request, model, action, params, (
        prepare(context, action, model, backend, row)
        for row in result
    ))


@getall.register()
def getall(
    context: Context,
    model: Model,
    backend: PostgreSQL,
):
    connection = context.get('transaction').connection
    table = backend.tables[model.manifest.name][model.name].main
    for row in connection.execute(sa.select([table])):
        yield dict(row)


@wipe.register()
def wipe(context: Context, model: Model, backend: PostgreSQL):
    authorize(context, Action.WIPE, model)

    connection = context.get('transaction').connection

    changes = backend.tables[model.manifest.name][model.name].changes
    connection.execute(changes.delete())

    main = backend.tables[model.manifest.name][model.name].main
    connection.execute(main.delete())


class utcnow(FunctionElement):
    type = sa.DateTime()


@compiles(utcnow, 'postgresql')
def pg_utcnow(element, compiler, **kw):
    return "TIMEZONE('utc', CURRENT_TIMESTAMP)"


def get_table_name(backend: PostgreSQL, manifest: str, name: str, table_type=MAIN_TABLE):
    assert isinstance(table_type, str)
    assert len(table_type) == 1
    assert table_type.isupper()

    # Table name construction depends on internal tables, so we must construct
    # internal table names differently.
    if manifest == 'internal':
        if table_type == MAIN_TABLE:
            return name
        else:
            return f'{name}_{table_type}'

    table = backend.tables['internal']['table'].main
    table_id = backend.get(backend.engine, table.c.id, table.c.name == name, default=None)
    if table_id is None:
        result = backend.engine.execute(
            table.insert(),
            name=name,
        )
        table_id = result.inserted_primary_key[0]
    name = unidecode.unidecode(name)
    name = PG_CLEAN_NAME_RE.sub('_', name)
    name = name.upper()
    name = name[:NAMEDATALEN - 6]
    name = name.rstrip('_')
    return f"{name}_{table_id:04d}{table_type}"


def get_changes_table(backend, table_name, id_type):
    table = sa.Table(
        table_name, backend.schema,
        sa.Column('change_id', BIGINT, primary_key=True),
        sa.Column('transaction_id', sa.Integer, sa.ForeignKey('transaction.id')),
        sa.Column('id', id_type),  # reference to main table
        sa.Column('datetime', sa.DateTime),
        sa.Column('action', sa.String(8)),  # insert, update, delete
        sa.Column('change', JSONB),
    )
    return table


@prepare.register()
def prepare(context: Context, action: Action, model: Model, backend: PostgreSQL, value: RowProxy, *, show: typing.List[str] = None) -> dict:
    return prepare(context, action, model, backend, dict(value), show=show)


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
