from spinta.datasets.backends.sql.components import Sql


class MySQL(Sql):
    type = "sql/mysql"

    query_builder_type = "sql/mysql"
