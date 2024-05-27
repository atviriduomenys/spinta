from spinta.backends.postgresql.ufuncs.result.components import PgResultBuilder
from spinta.cli.push import get_data_checksum
from spinta.components import Property
from spinta.core.ufuncs import ufunc, Expr
from spinta.ufuncs.basequerybuilder.components import Selected


@ufunc.resolver(PgResultBuilder, Expr)
def checksum(env: PgResultBuilder, expr: Expr):
    values = {}
    model = None
    if expr.args:
        for arg in expr.args:
            if arg.prop is None or not isinstance(arg.prop, Property):
                continue

            if model is None:
                model = arg.prop.model

            item = None
            if isinstance(arg, Selected) and arg.item is not None:
                item = env.data[arg.item]
            values[arg.prop.place] = item
    return get_data_checksum(values, model)
