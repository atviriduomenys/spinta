from spinta.datasets.backends.dataframe.backends.soap.components import Soap
from spinta.ufuncs.querybuilder.components import QueryBuilder


class SoapQueryBuilder(QueryBuilder):
    backend: Soap
