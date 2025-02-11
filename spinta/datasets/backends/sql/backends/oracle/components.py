from spinta.datasets.backends.sql.backends.oracle.ufuncs.query.components import OracleQueryBuilder
from spinta.datasets.backends.sql.components import Sql


class Oracle(Sql):
    type = 'sql/oracle'

    query_builder_class = OracleQueryBuilder
