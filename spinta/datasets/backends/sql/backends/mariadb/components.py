from spinta.datasets.backends.sql.backends.mariadb.ufuncs.query.components import MariaDBQueryBuilder
from spinta.datasets.backends.sql.components import Sql


class MariaDB(Sql):
    type = 'sql/mariadb'

    query_builder_class = MariaDBQueryBuilder
