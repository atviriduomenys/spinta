from spinta.datasets.backends.sql.backends.mysql.ufuncs.query.components import MySQLQueryBuilder
from spinta.datasets.backends.sql.components import Sql


class MySQL(Sql):
    type = 'sql/mysql'

    query_builder_class = MySQLQueryBuilder
