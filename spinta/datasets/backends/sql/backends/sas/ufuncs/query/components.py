from spinta.datasets.backends.sql.ufuncs.query.components import SqlQueryBuilder
from spinta.datasets.backends.sql.backends.sas.components import SAS


class SASQueryBuilder(SqlQueryBuilder):
    backend: SAS
