from spinta.datasets.backends.sql.components import Sql


class Sqlite(Sql):
    type = "sql/sqlite"

    query_builder_type = "sql/sqlite"
