import sqlalchemy as sa

from spinta.types import Function
from spinta.backends import Backend


class PostgreSQL(Backend):
    type = 'postgresql'
    name = None

    def __init__(self, name, config):
        assert isinstance(config, dict)
        assert config['type'] == self.type
        self.name = name
        self.engine = sa.create_engine(config['dsn'], echo=True)
        self.schema = sa.MetaData(self.engine)
        self.tables = {}  # populated by backend.prepare


class Prepare(Function):
    name = 'backend.prepare'
    types = ['manifest']
    backend = 'postgresql'

    def execute(self):
        for model_name, model in self.manifest.objects['model'].items():
            if self.manifest.config['backends'][model.backend]['type'] == self.backend.type:
                columns = []
                for prop_name, prop in model.properties.items():
                    if prop.type == 'pk':
                        column = sa.Column(prop_name, sa.Integer, primary_key=True)
                    elif prop.type == 'string':
                        column = sa.Column(prop_name, sa.String(255))
                    elif prop.type == 'date':
                        column = sa.Column(prop_name, sa.Date)
                    elif prop.type == 'integer':
                        column = sa.Column(prop_name, sa.Integer)
                    else:
                        self.error(f"Unknown property type {prop.type}.")
                    columns.append(column)
                self.backend.tables[model_name] = sa.Table(model_name, self.backend.schema, *columns)


class Migrate(Function):
    name = 'backend.migrate'
    types = ['manifest']
    backend = 'postgresql'

    def execute(self):
        self.engine.schema.create_all(checkfirst=True)
