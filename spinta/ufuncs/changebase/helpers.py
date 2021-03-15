from typing import Any

from spinta.components import Context
from spinta.components import Model
from spinta.core.ufuncs import Bind
from spinta.core.ufuncs import Expr
from spinta.ufuncs.changebase.components import ChangeModelBase
from spinta.ufuncs.components import ForeignProperty


def _re_change_expr(
    env: ChangeModelBase,
    fpr: ForeignProperty,
    expr: Any,
) -> Any:
    if isinstance(expr, Expr):
        args = [_re_change_expr(env, fpr, a) for a in expr.args]
        kwargs = {k: _re_change_expr(env, fpr, v) for k, v in expr.kwargs.items()}
        return type(expr)(expr.name, *args, **kwargs)

    if isinstance(expr, tuple):
        return tuple(_re_change_expr(env, fpr, v) for v in expr)

    if isinstance(expr, list):
        return [_re_change_expr(env, fpr, v) for v in expr]

    if isinstance(expr, dict):
        return {k: _re_change_expr(env, fpr, v) for k, v in expr}

    if isinstance(expr, ForeignProperty):
        return fpr.join(expr).get_bind_expr()

    if isinstance(expr, Bind):
        prop = env.model.properties[expr.name]
        return fpr.push(prop).get_bind_expr()

    return expr


def change_base_model(
    context: Context,
    model: Model,
    fpr: ForeignProperty,
) -> Expr:
    """Rewrite given expr by changing all binds to a new base model.

    """
    expr = model.external.prepare
    env = ChangeModelBase(context)
    env.update(model=model)
    expr = env.resolve(expr)
    return _re_change_expr(env, fpr, expr)
