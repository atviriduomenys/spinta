from typing import overload, Any

from spinta.core.ufuncs import ufunc, Expr, Env
from spinta.dimensions.enum.components import EnumFormula
from spinta.exceptions import FormulaError
from spinta.ufuncs.resultbuilder.components import EnumResultBuilder


@ufunc.resolver(EnumFormula, str)
def bind(env: EnumFormula, name: str) -> None:
    raise FormulaError(env.node, formula=name, error="Enum Binds (property names) are not supported.")


@overload
@ufunc.resolver(EnumResultBuilder, Expr)
def swap(env: EnumResultBuilder, expr: Expr) -> Any:
    """Should be invoked on ResultBuild. When data goes through."""
    args, kwargs = expr.resolve(env)
    new_value = env.call("swap", *args, **kwargs)
    if env.this != new_value:
        env.has_value_changed = True
    return new_value


@overload
@ufunc.resolver(Env, Expr)
def swap(env: Env, expr: Expr) -> Any:
    args, kwargs = expr.resolve(env)
    return Expr("swap", *args, **kwargs)
