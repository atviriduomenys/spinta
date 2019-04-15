import contextlib
import hashlib
import itertools
import re
import typing

import unidecode
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, BIGINT
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import FunctionElement

from spinta.types import NA
from spinta.commands import load, prepare, migrate, check, push, get, getall, wipe, error
from spinta.components import Context, BackendConfig, Manifest, Model, Property
from spinta.backends import Backend
from spinta.types.type import Type

# Maximum length for PostgreSQL identifiers (e.g. table names, column names,
# function names).
# https://github.com/postgres/postgres/blob/master/src/include/pg_config_manual.h
NAMEDATALEN = 63


PG_CLEAN_NAME_RE = re.compile(r'[^a-z0-9]+', re.IGNORECASE)

MAIN_TABLE = 'M'
CHANGES_TABLE = 'C'
CACHE_TABLE = 'T'

# Change actions
INSERT_ACTION = 'insert'
UPDATE_ACTION = 'update'


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
                result = self.engine.execute(
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
                raise Exception("No results where found.")
            else:
                return default

        else:
            raise Exception("Multiple rows were found.")


class ReadTransaction:

    def __init__(self, connection):
        self.connection = connection


class WriteTransaction(ReadTransaction):

    def __init__(self, connection, id):
        super().__init__(connection)
        self.id = id
        self.errors = 0


@load.register()
def load(context: Context, backend: PostgreSQL, config: BackendConfig):
    backend.name = config.name
    backend.engine = sa.create_engine(config.dsn, echo=False)
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
        for model in dataset.objects.values():
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
    changes_table = get_changes_table(backend, changes_table_name, sa.Integer)

    backend.tables[model.manifest.name][model.name] = ModelTables(main_table, changes_table)


@prepare.register()
def prepare(context: Context, backend: PostgreSQL, prop: Property):
    return prepare(context, backend, prop.type)


@prepare.register()
def prepare(context: Context, backend: PostgreSQL, type: Type):
    if type.prop.name == 'id':
        if type.name == 'integer':
            return sa.Column(type.prop.name, BIGINT, primary_key=True)
        else:
            context.error(f"Unsuported type {type.prop.name!r} for primary key.")
    elif type.name == 'type':
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
    elif type.name == 'ref':
        return _get_foreign_key(backend, type.prop.parent, type.prop)
    elif type.name == 'backref':
        return
    elif type.name == 'generic':
        return
    elif type.name == 'array':
        return
    else:
        raise Exception(
            f"Unknown property type {type.name} for {type.prop.name} property "
            f"of {type.prop.parent.name} model in {type.prop.path}."
        )


@error.register()
def error(exc: Exception, context: Context, backend: PostgreSQL, type: Type):
    error(exc, context, type)


@error.register()
def error(exc: Exception, context: Context, model: Model, backend: PostgreSQL):
    error(exc, context, model)


def _get_foreign_key(backend: PostgreSQL, model: Model, prop: Property):
    ref_model = model.manifest.objects['model'][prop.object]
    table_name = get_table_name(backend, ref_model.manifest.name, ref_model.name)
    pkey_name = ref_model.get_primary_key().name
    return [
        sa.Column(prop.name, sa.Integer),
        sa.ForeignKeyConstraint(
            [prop.name], [f'{table_name}.{pkey_name}'],
            name=_get_pg_name(f'fk_{model.name}_{prop.name}'),
        )
    ]


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
    backend.schema.create_all(checkfirst=True)


@check.register()
def check(context: Context, model: Model, backend: PostgreSQL, data: dict):
    connection = context.get('transaction').connection
    table = backend.tables[model.manifest.name][model.name].main
    action = 'update' if 'id' in data else 'insert'

    for name, prop in model.properties.items():
        if prop.required and name not in data:
            raise Exception(f"{name!r} is required for {model}.")

        if prop.unique and prop.name in data:
            if action == 'update':
                condition = sa.and_(
                    table.c[prop.name] == data[prop.name],
                    table.c['id'] != data['id'],
                )
            else:
                condition = table.c[prop.name] == data[prop.name]
            na = object()
            result = backend.get(connection, table.c[prop.name], condition, default=na)
            if result is not na:
                raise Exception(f"{name!r} is unique for {model} and a duplicate value is found in database.")


@push.register()
def push(context: Context, model: Model, backend: PostgreSQL, data: dict):
    transaction = context.get('transaction')
    connection = transaction.connection
    table = backend.tables[model.manifest.name][model.name]

    data = {
        k: v for k, v in data.items() if k in table.main.columns
    }

    # Update existing row.
    if 'id' in data:
        action = UPDATE_ACTION
        result = connection.execute(
            table.main.update().
            where(table.main.c.id == data['id']).
            values(data)
        )
        if result.rowcount == 1:
            row_id = data['id']
        elif result.rowcount == 0:
            raise Exception("Update failed, {self.obj} with {data['id']} not found.")
        else:
            raise Exception("Update failed, {self.obj} with {data['id']} has found and update {result.rowcount} rows.")

    # Insert new row.
    else:
        action = INSERT_ACTION
        result = connection.execute(
            table.main.insert().values(data),
        )
        row_id = result.inserted_primary_key[0]

    # Track changes.
    connection.execute(
        table.changes.insert().values(
            transaction_id=transaction.id,
            id=row_id,
            datetime=utcnow(),
            action=action,
            change={k: v for k, v in data.items() if k not in {'id'}},
        ),
    )

    return row_id


@get.register()
def get(context: Context, model: Model, backend: PostgreSQL, id: int):
    connection = context.get('transaction').connection
    table = backend.tables[model.manifest.name][model.name].main
    result = backend.get(connection, table, table.c.id == id)
    result = {k: v for k, v in result.items() if not k.startswith('_')}
    result['type'] = model.name
    return result


@getall.register()
def getall(context: Context, model: Model, backend: PostgreSQL):
    connection = context.get('transaction').connection
    table = backend.tables[model.manifest.name][model.name].main
    result = connection.execute(sa.select([table]))
    for row in result:
        yield {k: v for k, v in row.items() if not k.startswith('_')}


@wipe.register()
def wipe(context: Context, model: Model, backend: PostgreSQL):
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
