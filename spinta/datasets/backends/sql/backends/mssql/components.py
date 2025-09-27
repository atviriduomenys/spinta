from spinta.datasets.backends.sql.components import Sql


class MSSQL(Sql):
    type = "sql/mssql"

    query_builder_type = "sql/mssql"
