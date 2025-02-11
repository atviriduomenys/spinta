from spinta.datasets.backends.sql.backends.sqlite.ufuncs.query.components import SqliteQueryBuilder
from spinta.datasets.backends.sql.components import Sql


class Sqlite(Sql):
    type = 'sql/sqlite'

    query_builder_class = SqliteQueryBuilder
