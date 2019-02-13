import contextlib
import datetime
import hashlib
import itertools
import re
import typing

import unidecode
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import FunctionElement

from spinta.types import NA
from spinta.commands import Command
from spinta.backends import Backend

# Maximum length for PostgreSQL identifiers (e.g. table names, column names,
# function names).
# https://github.com/postgres/postgres/blob/master/src/include/pg_config_manual.h
NAMEDATALEN = 63


PG_CLEAN_NAME_RE = re.compile(r'[^a-z0-9]+', re.IGNORECASE)

MAIN_TABLE = 'M'
CHANGES_TABLE = 'C'


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
                table = self.tables['transaction']
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


class LoadBackend(Command):
    metadata = {
        'name': 'manifest.load',
        'type': 'postgresql',
    }

    def execute(self):
        super().execute()
        self.obj.engine = sa.create_engine(self.obj.dsn, echo=False)
        self.obj.schema = sa.MetaData(self.obj.engine)
        self.obj.tables = {}


class Prepare(Command):
    metadata = {
        'name': 'backend.prepare',
        'type': 'manifest',
        'backend': 'postgresql',
    }

    def execute(self):
        for model in self.store.objects[self.ns]['model'].values():
            if self.store.config.backends[model.backend].type == self.backend.type:
                columns = []
                for prop_name, prop in model.properties.items():
                    if prop.type == 'pk':
                        columns.append(
                            sa.Column(prop_name, sa.Integer, primary_key=True)
                        )
                    elif prop.type == 'string':
                        columns.append(
                            sa.Column(prop_name, sa.Text)
                        )
                    elif prop.type == 'date':
                        columns.append(
                            sa.Column(prop_name, sa.Date)
                        )
                    elif prop.type == 'datetime':
                        columns.append(
                            sa.Column(prop_name, sa.DateTime)
                        )
                    elif prop.type == 'integer':
                        columns.append(
                            sa.Column(prop_name, sa.Integer)
                        )
                    elif prop.type == 'ref':
                        columns.extend(
                            self.get_foreign_key(model, prop)
                        )
                    else:
                        self.error(f"Unknown property type {prop.type}.")
                self.create_model_tables(model, columns)

    def create_model_tables(self, model, columns):
        # Create main table.
        main_table_name = self.get_table_name(model, MAIN_TABLE)
        main_table = sa.Table(
            main_table_name, self.backend.schema,
            sa.Column('_transaction_id', sa.Integer, sa.ForeignKey('transaction.id')),
            *columns,
        )

        # Create changes table.
        changes_table_name = self.get_table_name(model, CHANGES_TABLE)
        changes_table = sa.Table(
            changes_table_name, self.backend.schema,
            sa.Column('change_id', sa.Integer, primary_key=True),
            sa.Column('transaction_id', sa.Integer, sa.ForeignKey('transaction.id')),
            sa.Column('id', sa.Integer),  # reference to main table
            sa.Column('datetime', sa.DateTime),
            sa.Column('action', sa.String(8)),  # insert, update, delete
            sa.Column('change', JSONB),
        )

        self.backend.tables[model.name] = ModelTables(main_table, changes_table)

    def get_model(self, model_name):
        return self.store.objects[self.ns]['model'][model_name]

    def get_foreign_key(self, model, prop):
        ref_model = self.get_model(prop.object)
        table_name = self.get_table_name(ref_model)
        pkey_name = ref_model.get_primary_key().name
        return [
            sa.Column(prop.name, sa.Integer),
            sa.ForeignKeyConstraint(
                [prop.name], [f'{table_name}.{pkey_name}'],
                name=self.get_pg_name(f'fk_{table_name}_{pkey_name}'),
            )
        ]

    def get_table_name(self, model, table_type=MAIN_TABLE):
        assert isinstance(table_type, str)
        assert len(table_type) == 1
        assert table_type.isupper()
        table = self.backend.tables['model'].main
        model_id = self.backend.get(self.backend.engine, table.c.id, table.c.name == model.name, default=None)
        if model_id is None:
            result = self.backend.engine.execute(
                table.insert(),
                name=model.name,
                date=datetime.datetime.utcnow(),
                version=model.version,
            )
            model_id = result.inserted_primary_key[0]
        name = unidecode.unidecode(model.name)
        name = PG_CLEAN_NAME_RE.sub('_', name)
        name = name.upper()
        name = name[:NAMEDATALEN - 6]
        name = name.rstrip('_')
        return f"{name}_{model_id:04d}{table_type}"

    def get_pg_name(self, name):
        if len(name) > NAMEDATALEN:
            name_hash = hashlib.sha256(name.encode()).hexdigest()
            return name[:NAMEDATALEN - 7] + '_' + name_hash[-6:]
        else:
            return name


class PrepareInternal(Prepare):
    metadata = {
        'name': 'backend.prepare.internal',
        'type': 'manifest',
        'backend': 'postgresql',
    }

    def get_table_name(self, model, table_type=MAIN_TABLE):
        if table_type == MAIN_TABLE:
            # Don not use table type suffix for the main table.
            return model.name
        else:
            return f'{model.name}_{table_type}'


class ModelTables(typing.NamedTuple):
    main: sa.Table
    changes: sa.Table = None


class Migrate(Command):
    metadata = {
        'name': 'backend.migrate',
        'type': 'manifest',
        'backend': 'postgresql',
    }

    def execute(self):
        self.backend.schema.create_all(checkfirst=True)


class MigrateInternal(Migrate):
    metadata = {
        'name': 'backend.migrate.internal',
        'type': 'manifest',
        'backend': 'postgresql',
    }


class Check(Command):
    metadata = {
        'name': 'check',
        'type': 'model',
        'backend': 'postgresql',
    }

    def execute(self):
        connection = self.args.transaction.connection
        data = self.args.data
        table = self.backend.tables[self.obj.name].main
        action = 'update' if 'id' in data else 'insert'

        for name, prop in self.obj.properties.items():
            if prop.required and name not in data:
                raise Exception(f"{name!r} is required for {self.obj}.")

            if prop.unique and prop.name in data:
                if action == 'update':
                    condition = sa.and_(
                        table.c[prop.name] == data[prop.name],
                        table.c['id'] != data['id'],
                    )
                else:
                    condition = table.c[prop.name] == data[prop.name]
                na = object()
                result = self.backend.get(connection, table.c[prop.name], condition, default=na)
                if result is not na:
                    raise Exception(f"{name!r} is unique for {self.obj} and a duplicate value is found in database.")


class Push(Command):
    metadata = {
        'name': 'push',
        'type': 'model',
        'backend': 'postgresql',
    }

    def execute(self):
        transaction = self.args.transaction
        connection = transaction.connection
        data = self.args.data
        table = self.backend.tables[self.obj.name]

        # Update existing row.
        if 'id' in data:
            action = 'update'
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
            action = 'insert'
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


class Get(Command):
    metadata = {
        'name': 'get',
        'type': 'model',
        'backend': 'postgresql',
    }

    def execute(self):
        connection = self.args.transaction.connection
        table = self.backend.tables[self.obj.name].main
        result = self.backend.get(connection, table, table.c.id == self.args.id)
        return {k: v for k, v in result.items() if not k.startswith('_')}


class GetAll(Command):
    metadata = {
        'name': 'getall',
        'type': 'model',
        'backend': 'postgresql',
    }

    def execute(self):
        connection = self.args.transaction.connection
        table = self.backend.tables[self.obj.name].main

        result = connection.execute(
            sa.select([table])
        )

        for row in result:
            yield {k: v for k, v in row.items() if not k.startswith('_')}


class Wipe(Command):
    metadata = {
        'name': 'wipe',
        'type': 'model',
        'backend': 'postgresql',
    }

    def execute(self):
        connection = self.args.transaction.connection

        changes = self.backend.tables[self.obj.name].changes
        connection.execute(changes.delete())

        main = self.backend.tables[self.obj.name].main
        connection.execute(main.delete())


class utcnow(FunctionElement):
    type = sa.DateTime()


@compiles(utcnow, 'postgresql')
def pg_utcnow(element, compiler, **kw):
    return "TIMEZONE('utc', CURRENT_TIMESTAMP)"
