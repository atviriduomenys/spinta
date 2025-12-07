from spinta.datasets.backends.sql.components import Sql


class PostgreSQL(Sql):
    type = "sql/postgresql"

    query_builder_type = "sql/postgresql"
