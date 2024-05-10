from spinta.core.ufuncs import ufunc
from spinta.ufuncs.resultbuilder.components import ResultBuilder


@ufunc.resolver(ResultBuilder)
def count(env: ResultBuilder):
    pass
