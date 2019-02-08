import contextlib
import datetime
import hashlib
import itertools
import re

import unidecode
import sqlalchemy as sa

from spinta.types import NA
from spinta.commands import Command
from spinta.backends import Backend
from spinta.types.type import ManifestLoad

# Maximum length for PostgreSQL identifiers (e.g. table names, column names,
# function names).
# https://github.com/postgres/postgres/blob/master/src/include/pg_config_manual.h
NAMEDATALEN = 63


PG_CLEAN_NAME_RE = re.compile(r'[^a-z0-9]+', re.IGNORECASE)


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
    def transaction(self):
        with self.engine.begin() as connection:
            yield connection

    def get(self, connection, columns, condition, default=NA):
        if isinstance(columns, list):
            scalar = False
        else:
            scalar = True
            columns = [columns]

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


class ManifestLoadBackend(ManifestLoad):
    metadata = {
        'name': 'manifest.load',
        'type': 'postgresql',
    }

    def execute(self):
        super().execute()
        self.obj.engine = sa.create_engine(self.obj.dsn, echo=True)
        self.obj.schema = sa.MetaData(self.obj.engine)
        self.obj.tables = {}


class Prepare(Command):
    metadata = {
        'name': 'backend.prepare',
        'type': 'manifest',
        'backend': 'postgresql',
    }

    def execute(self):
        for model_name, model in self.store.objects[self.ns]['model'].items():
            if self.store.config.backends[model.backend].type == self.backend.type:
                columns = []
                for prop_name, prop in model.properties.items():
                    if prop.type == 'pk':
                        columns.append(
                            sa.Column(prop_name, sa.Integer, primary_key=True)
                        )
                    elif prop.type == 'string':
                        columns.append(
                            sa.Column(prop_name, sa.String(255))
                        )
                    elif prop.type == 'date':
                        columns.append(
                            sa.Column(prop_name, sa.Date)
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
                table_name = self.get_table_name(model)
                self.backend.tables[model_name] = sa.Table(table_name, self.backend.schema, *columns)

    def get_model(self, model_name):
        return self.store.objects[self.ns]['model'][model_name]

    def get_foreign_key(self, model, prop):
        ref_model = self.get_model(prop.object)
        table_name = self.get_table_name(ref_model)
        pkey_name = self.get_model_primary_key(ref_model).name
        return [
            sa.Column(prop.name, sa.Integer),
            sa.ForeignKeyConstraint(
                [prop.name], [f'{table_name}.{pkey_name}'],
                name=self.get_pg_name(f'fk_{table_name}_{pkey_name}'),
            )
        ]

    def get_model_primary_key(self, model):
        for prop in model.properties.values():
            if prop.type == 'pk':
                return prop
        raise Exception(f"{model} does not have a primary key.")

    def get_table_name(self, model):
        table = self.backend.tables['model']
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
        name = name[:NAMEDATALEN - 5]
        name = name.rstrip('_')
        return f"{name}_{model_id:04d}"

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

    def get_table_name(self, model):
        return model.name


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
        connection = self.args.connection
        data = self.args.data
        table = self.backend.tables[self.obj.name]

        for name, prop in self.obj.properties.items():
            if prop.required and name not in data:
                raise Exception(f"{name!r} is required for {self.obj}.")

            if prop.unique:
                na = object()
                result = self.backend.get(connection, table.c[prop.name], table.c[prop.name] == data[prop.name], default=na)
                if result is not na:
                    raise Exception(f"{name!r} is unique for {self.obj} and a duplicate value is found in database.")


class Push(Command):
    metadata = {
        'name': 'push',
        'type': 'model',
        'backend': 'postgresql',
    }

    def execute(self):
        connection = self.args.connection
        data = self.args.data
        table = self.backend.tables[self.obj.name]

        if 'id' in data:
            result = connection.execute(
                table.update().
                where(table.c.id == data['id']).
                values(data)
            )
            if result.rowcount == 1:
                return data['id']
            elif result.rowcount == 0:
                raise Exception("Update failed, {self.obj} with {data['id']} not found.")
            else:
                raise Exception("Update failed, {self.obj} with {data['id']} has found and update {result.rowcount} rows.")
        else:
            result = connection.execute(
                table.insert().values(data),
            )
            return result.inserted_primary_key[0]
