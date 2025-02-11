from spinta.datasets.backends.sql.backends.mssql.ufuncs.query.components import MSSQLQueryBuilder
from spinta.datasets.backends.sql.components import Sql


class MSSQL(Sql):
    type = 'sql/mssql'

    query_builder_class = MSSQLQueryBuilder
