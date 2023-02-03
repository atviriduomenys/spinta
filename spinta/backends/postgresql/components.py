from typing import Dict
from typing import Union

import contextlib
import itertools
import uuid

import sqlalchemy as sa
from sqlalchemy.engine import Engine

from spinta.backends.postgresql.helpers import get_column_name
from spinta.types.datatype import Ref
from spinta.utils.schema import NA
from spinta.components import Model, Property
from spinta.backends.constants import TableType
from spinta.backends.components import Backend, BackendFeatures
from spinta.backends.helpers import get_table_name
from spinta.backends.postgresql.sqlalchemy import utcnow
from spinta.exceptions import MultipleRowsFound, NotFoundError
from spinta.exceptions import PropertyNotFound


class PostgreSQL(Backend):
    metadata = {
        'name': 'postgresql',
        'properties': {
            'dsn': {'type': 'string', 'required': True},
        },
    }

    features = {
        BackendFeatures.FILE_BLOCKS,
        BackendFeatures.WRITE,
    }

    engine: Engine = None
    schema: sa.MetaData = None
    tables: Dict[str, sa.Table] = None

    @contextlib.contextmanager
    def transaction(self, write=False):
        with self.engine.begin() as connection:
            if write:
                table = self.tables['_txn']
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

    @contextlib.contextmanager
    def begin(self):
        with self.engine.begin() as conn:
            yield conn

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
        name = get_table_name(node, ttype)
        assert name not in self.tables, name
        self.tables[name] = table

    def get_table(
        self,
        node: Union[Model, Property],
        ttype: TableType = TableType.MAIN,
        *,
        fail: bool = True,
    ):
        name = get_table_name(node, ttype)
        if fail:
            return self.tables[name]
        else:
            return self.tables.get(name)

    def get_column(self, table: sa.Table, prop: Property, *, select=False) -> sa.Column:
        if prop.list is not None:
            table = self.get_table(prop.list, TableType.LIST)
        column = get_column_name(prop)
        if isinstance(prop.dtype, Ref):
            # TODO: Move this into a command.
            column += '._id'
        if column not in table.c:
            raise PropertyNotFound(prop.dtype)
        return table.c[column]

    def query_nodes(self):
        return []

    def bootstrapped(self):
        meta = sa.MetaData(self.engine)
        table = sa.Table('_schema', meta)
        insp = sa.inspect(self.engine)
        if insp.has_table(table.name):
            with self.engine.begin() as conn:
                query = sa.select([sa.func.count()]).select_from(table)
                return conn.execute(query).scalar() > 0
        return False


class ReadTransaction:
    id: str
    errors: int

    def __init__(self, connection):
        self.connection = connection


class WriteTransaction(ReadTransaction):

    def __init__(self, connection, id_: str):
        super().__init__(connection)
        self.id = id_
        self.errors = 0
