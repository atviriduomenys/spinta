from spinta import commands, exceptions
from spinta.components import Context, Model
from spinta.core.ufuncs import Expr
from spinta.datasets.backends.dataframe.components import DaskBackend
from spinta.datasets.backends.dataframe.ufuncs.query.ufuncs import COMPARE
from spinta.handlers import ErrorManager


def _get_compare_operators(expr: Expr) -> list[str]:
    """Recursively collect compare operator (filters) names from an Expr tree."""
    if not isinstance(expr, Expr):
        return []
    filters: list[str] = []
    if expr.name in COMPARE:
        filters.append(expr.name)
    for arg in expr.args:
        filters.extend(_get_compare_operators(arg))
    for val in expr.kwargs.values():
        filters.extend(_get_compare_operators(val))
    return filters


@commands.check.register(Context, Model, DaskBackend)
def check(context: Context, model: Model, backend: DaskBackend) -> None:
    # If there are any compare operators in the prepare expression,
    # raise an error since they are not supported for Dask backend.
    if (
        model.external
        and model.external.prepare
        and (compare_operators := list(dict.fromkeys(_get_compare_operators(model.external.prepare))))
    ):
        manager: ErrorManager = context.get("error_manager")
        manager.handle_error(exceptions.DaskBackendCompareNotSupported(model, operators=compare_operators))
