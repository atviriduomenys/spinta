from spinta.datasets.backends.sql.components import Sql


class Oracle(Sql):
    type = "sql/oracle"

    query_builder_type = "sql/oracle"
