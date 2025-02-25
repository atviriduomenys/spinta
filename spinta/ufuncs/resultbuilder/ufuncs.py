from spinta.core.ufuncs import ufunc, Expr
from spinta.ufuncs.querybuilder.components import Selected
from spinta.ufuncs.resultbuilder.components import ResultBuilder


@ufunc.resolver(ResultBuilder)
def count(env: ResultBuilder):
    pass


@ufunc.resolver(ResultBuilder, Selected, Selected)
def point(env: ResultBuilder, x: Selected, y: Selected) -> str:
    x = env.data[x.item]
    y = env.data[y.item]
    return f'POINT ({x} {y})'


@ufunc.resolver(ResultBuilder, Expr)
def split(env: ResultBuilder, expr: Expr):
    args, kwargs = expr.resolve(env)

    if len(args) != 1:
        raise Exception("SPLIT REQUIRES ONLY 1 ARGUMENT")

    separator = args[0]
    return env.this.split(separator)
