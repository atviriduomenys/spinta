from spinta.datasets.backends.sql.components import Sql


class MariaDB(Sql):
    type = "sql/mariadb"

    query_builder_type = "sql/mariadb"
