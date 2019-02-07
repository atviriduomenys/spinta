import datetime
import hashlib
import itertools
import re

import unidecode
import sqlalchemy as sa

from spinta.types import NA
from spinta.commands import Command
from spinta.backends import Backend

# Maximum length for PostgreSQL identifiers (e.g. table names, column names,
# function names).
# https://github.com/postgres/postgres/blob/master/src/include/pg_config_manual.h
NAMEDATALEN = 63


PG_CLEAN_NAME_RE = re.compile(r'[^a-z0-9]+', re.IGNORECASE)


class PostgreSQL(Backend):
    type = 'postgresql'
    name = None

    def __init__(self, name, config):
        assert isinstance(config, dict)
        assert config['type'] == self.type
        self.name = name
        self.engine = sa.create_engine(config['dsn'], echo=True)
        self.schema = sa.MetaData(self.engine)
        self.tables = {}  # populated by backend.prepare and backend.prepare.internal

    def get(self, columns, condition, default=NA):
        if isinstance(columns, list):
            scalar = False
        else:
            scalar = True
            columns = [columns]

        result = self.engine.execute(
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


class Prepare(Command):
    metadata = {
        'name': 'backend.prepare',
        'type': 'manifest',
        'backend': 'postgresql',
    }

    def execute(self):
        for model_name, model in self.store.objects[self.ns]['model'].items():
            if self.store.config['backends'][model.backend]['type'] == self.backend.type:
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
        model_id = self.backend.get(table.c.id, table.c.name == model.name, default=None)
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
