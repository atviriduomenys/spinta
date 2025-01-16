from spinta.core.ufuncs import ufunc
from spinta.ufuncs.basequerybuilder.components import Selected
from spinta.ufuncs.resultbuilder.components import ResultBuilder


@ufunc.resolver(ResultBuilder)
def count(env: ResultBuilder):
    pass


@ufunc.resolver(ResultBuilder, Selected, Selected)
def point(env: ResultBuilder, x: Selected, y: Selected) -> str:
    x = env.data[x.item]
    y = env.data[y.item]
    return f'POINT ({x} {y})'
