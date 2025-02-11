from spinta.datasets.backends.sql.backends.postgresql.ufuncs.query.components import PostgreSQLQueryBuilder
from spinta.datasets.backends.sql.components import Sql


class PostgreSQL(Sql):
    type = 'sql/postgresql'

    query_builder_class = PostgreSQLQueryBuilder
